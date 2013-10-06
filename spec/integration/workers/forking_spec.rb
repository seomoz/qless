# Encoding: utf-8

# The things we're testing
require 'qless'
require 'qless/worker'
require 'qless/job_reservers/round_robin'
require 'qless/middleware/retry_exceptions'
require 'securerandom'

# Spec stuff
require 'spec_helper'
require 'qless/test_helpers/worker_helpers'

module Qless
  describe Workers::ForkingWorker, :integration do
    include Qless::WorkerHelpers

    let(:key) { :worker_integration_job }
    let(:queue) { client.queues['main'] }
    let(:reserver) { Qless::JobReservers::RoundRobin.new([queue]) }
    let(:worker) do
      Qless::Workers::ForkingWorker.new(
        Qless::JobReservers::RoundRobin.new([queue]),
        interval: 1,
        max_startup_interval: 0,
        allowed_memory_multiple: 5,
        log_level: Logger::DEBUG)
    end

    it 'can start a worker and then shut it down' do
      # A job that just puts a word in a redis list to show that its done
      job_class = Class.new do
        def self.perform(job)
          Redis.connect(url: job['redis']).rpush(job['key'], job['word'])
        end
      end
      stub_const('JobClass', job_class)

      # Make jobs for each word
      words = %w{foo bar howdy}
      words.each do |word|
        queue.put('JobClass', { redis: redis.client.id, key: key, word: word })
      end

      # Wait for the job to complete, and then kill the child process
      run_worker_concurrently_with(worker) do
        words.each do |word|
          client.redis.brpop(key, timeout: 1).should eq([key.to_s, word])
        end
      end
    end

    it 'can drain its queues and exit' do
      job_class = Class.new do
        def self.perform(job)
          job.client.redis.rpush(job['key'], job['word'])
        end
      end

      stub_const('JobClass', job_class)

      # Make jobs for each word
      words = %w{foo bar howdy}
      words.each do |word|
        queue.put('JobClass', { key: key, word: word })
      end

      drain_worker_queues(worker)
      expect(client.redis.lrange(key, 0, -1)).to eq(words)
    end

    it 'does not blow up when the child process exits unexpectedly' do
      # A job that falls on its sword until its last retry
      job_class = Class.new do
        def self.perform(job)
          if job.retries_left > 1
            job.retry
            Process.kill(9, Process.pid)
          else
            Redis.connect(url: job['redis']).rpush(job['key'], job['word'])
          end
        end
      end
      stub_const('JobClass', job_class)

      # Put a job and run it, making sure it finally succeeds
      queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                retries: 5)
      run_worker_concurrently_with(worker) do
        client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
      end
    end

    it 'passes along middleware to child processes' do
      # Our mixin module sends a message to a channel
      mixin = Module.new do
        define_method :around_perform do |job|
          Redis.connect(url: job['redis']).rpush(job['key'], job['word'])
          super(job)
        end
      end
      worker.extend(mixin)

      # Our job class does nothing
      job_class = Class.new do
        def self.perform(job); end
      end
      stub_const('JobClass', job_class)

      # Put a job in and run it
      queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo })
      run_worker_concurrently_with(worker) do
        client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
      end
    end

    it 'has a usable after_fork hook for use with middleware' do
      # Our job class does nothing
      job_class = Class.new do
        def self.perform(job)
          Redis.connect(url: job['redis']).rpush(job['key'], 'job')
        end
      end
      stub_const('JobClass', job_class)

      # Make jobs for each word
      3.times do
        queue.put('JobClass', { redis: redis.client.id, key: key })
      end

      # mixin module sends a message to a channel
      redis_url = self.redis_url
      key = self.key
      mixin = Module.new do
        define_method :after_fork do
          Redis.connect(url: redis_url).rpush(key, 'after_fork')
          super()
        end
      end
      worker.extend(mixin)

      # Wait for the job to complete, and then kill the child process
      drain_worker_queues(worker)
      words = redis.lrange(key, 0, -1)
      expect(words).to eq %w[ after_fork job job job ]
    end

    it 'does not allow a bloated job to cause a child to permanently retain the memory blot' do
      bloated_job_class = Class.new do
        def self.perform(job)
          job_record = JobRecord.new(Process.pid, Qless.current_memory_usage_in_kb, nil)
          job_record.after_mem = bloat_memory(job_record.before_mem, job.data.fetch("bloat_factor"))

          # publish what the memory usage was before/after
          job.client.redis.rpush('mem_usage', Marshal.dump(job_record))
        end

        def self.bloat_memory(original_mem, target_multiple)
          current_mem = original_mem
          target = original_mem * target_multiple

          while current_mem < target
            SecureRandom.hex(
              # The * 300 is based on experimentation, taking into account
              # the fact that target/current are in KB.
              (target - current_mem) * 100
            ).to_sym # symbols are never GC'd.

            current_mem = Qless.current_memory_usage_in_kb
          end

          current_mem
        end
      end

      stub_const("JobRecord", Struct.new(:pid, :before_mem, :after_mem))

      stub_const('BloatedJobClass', bloated_job_class)

      [1.5, 4, 1].each do |bloat_factor|
        queue.put(BloatedJobClass, { bloat_factor: bloat_factor })
      end

      job_records = []

      run_worker_concurrently_with(worker) do
        3.times do
          _, result = client.redis.brpop('mem_usage', timeout: 5)
          job_records << Marshal.load(result)
        end
      end

      # the second job should increase mem growth but be the same pid.
      expect(job_records[1].pid).to eq(job_records[0].pid)
      expect(job_records[1].before_mem).to be > job_records[0].before_mem

      # the third job sould be a new process with cleared out memory
      expect(job_records[2].pid).not_to eq(job_records[0].pid)
      expect(job_records[2].before_mem).to be < job_records[1].before_mem
    end

    context 'when a job times out', :uses_threads do
      it 'fails the job with an error containing the job backtrace' do
        pending('I do not think this is actually the desired behavior')
      end
    end
  end
end
