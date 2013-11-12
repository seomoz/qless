# Encoding: utf-8

require 'spec_helper'
require 'qless/middleware/retry_exceptions'
require 'support/forking_worker_context'

module Qless
  describe Workers::ForkingWorker do
    include_context "forking worker"

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

    context 'when a job times out', :uses_threads do
      it 'fails the job with an error containing the job backtrace' do
        pending('I do not think this is actually the desired behavior')
      end
    end
  end
end
