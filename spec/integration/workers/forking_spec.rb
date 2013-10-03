# Encoding: utf-8

# The things we're testing
require 'qless'
require 'qless/worker'
require 'qless/job_reservers/round_robin'
require 'qless/middleware/retry_exceptions'

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
      thread_worker(worker) do
        words.each do |word|
          client.redis.brpop(key, timeout: 1).should eq([key.to_s, word])
        end
      end
    end

    it 'does not blow up when the child process exits unexpectedly' do
      # We need to turn off grace-period and have jobs immediately time out
      client.config['grace-period'] = 0
      queue.heartbeat = -100

      # A job that falls on its sword until its last retry
      job_class = Class.new do
        def self.perform(job)
          if job.retries_left > 1
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
      thread_worker(worker) do
        client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
      end
    end

    it 'passes along middleware to child processes' do
      # Our mixin module sends a message to a channel
      mixin = Module.new do
        define_method :around_perform do |job|
          Redis.connect(url: job['redis']).rpush(job['key'], job['word'])
          super
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
      thread_worker(worker) do
        client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
      end
    end

    context 'when a job times out', :uses_threads do
      it 'fails the job with an error containing the job backtrace' do
        pending('I do not think this is actually the desired behavior')
      end
    end
  end
end
