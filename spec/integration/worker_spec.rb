# Encoding: utf-8

# The things we're testing
require 'qless'
require 'qless/worker'
require 'qless/middleware/retry_exceptions'

# Spec stuff
require 'spec_helper'

module Qless
  describe Worker, :integration do
    let(:key) { :worker_integration_job }
    let(:queue) { client.queues['main'] }
    let(:worker) do
      Qless::Worker.new(
        Qless::JobReservers::RoundRobin.new([queue]),
        interval: 1,
        max_startup_interval: 0)
    end

    # Yield with a worker running, and then clean the worker up afterwards
    def fork_worker
      child = fork do
        worker.run
      end

      begin
        yield
      ensure
        Process.kill('TERM', child)
        Process.wait(child)
      end
    end

    # Yield with a worker running in a thread, clean up after
    def thread_worker
      thread = Thread.new do
        begin
          worker.run
        rescue RuntimeError
        ensure
          worker.stop!('TERM')
        end
      end

      begin
        yield
      ensure
        thread.raise('stop')
        thread.join
      end
    end

    # Run the worker in this thread
    def work
      worker.work(0)
    end

    it 'does not leak threads' do
      queue.put('Foo', word: 'foo')
      expect { work }.not_to change { Thread.list }
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
      thread_worker do
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
      thread_worker do
        client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
      end
    end

    context 'when a job times out', :uses_threads do
      it 'takes a new job' do
        # We need to disable the grace period so it's immediately available
        client.config['grace-period'] = 0

        # Job that sleeps for a while on the first pass
        job_class = Class.new do
          def self.perform(job)
            redis = Redis.connect(url: job['redis'])
            if redis.get(job['jid']).nil?
              redis.set(job['jid'], '1')
              redis.rpush(job['key'], job['word'])
              sleep 5
              job.fail('foo', 'bar')
            else
              job.complete
            end
          end
        end
        stub_const('JobClass', job_class)

        # Put this job into the queue and then busy-wait for the job to be
        # running, time it out, then make sure it eventually completes
        queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                  jid: 'jid')
        thread_worker do
          client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
          client.jobs['jid'].timeout
          while %w{stalled waiting running}.include?(client.jobs['jid'].state)
          end
          client.jobs['jid'].state.should eq('complete')
        end
      end

      it 'does not blow up for jobs it does not have' do
        # Disable grace period
        client.config['grace-period'] = 0

        # A class that sends a message and sleeps for a bit
        job_class = Class.new do
          def self.perform(job)
            Redis.connect(url: job['redis']).rpush(job['key'], job['word'])
            sleep 10
          end
        end
        stub_const('JobClass', job_class)

        # Put this job into the queue and then have the worker lose its lock
        queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                  priority: 10, jid: 'jid')
        queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                  priority: 5)
        
        thread_worker do
          # Busy-wait for the job to be running, and then time out another job
          client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
          job = queue.pop
          expect(job.jid).to_not eq('jid')
          # And the first should still be running
          client.jobs['jid'].state.should eq('running')
        end
      end

      it 'fails the job with an error containing the job backtrace' do
        pending('I do not think this is actually the desired behavior')
      end
    end

    # Specs related specifically to middleware
    describe Qless::Middleware do
      it 'will retry and eventually fail a repeatedly failing job' do
      # A job that raises an error, but automatically retries that error type
      job_class = Class.new do
        extend Qless::Job::SupportsMiddleware
        extend Qless::Middleware::RetryExceptions

        Kaboom = Class.new(StandardError)
        retry_on Kaboom

        def self.perform(job)
          Redis.connect(url: job['redis']).rpush(job['key'], job['word'])
          raise Kaboom
        end
      end
      stub_const('JobClass', job_class)

      # Put a job and run it, making sure it gets retried
      queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                jid: 'jid', retries: 10)
      thread_worker do
        client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
        until client.jobs['jid'].state == 'waiting'; end
      end
      expect(client.jobs['jid'].retries_left).to be < 10
    end
    end
  end
end
