# Encoding: utf-8

# The things we're testing
require 'qless'
require 'qless/worker/serial'
require 'qless/job_reservers/round_robin'
require 'qless/middleware/retry_exceptions'

# Spec stuff
require 'spec_helper'
require 'qless/test_helpers/worker_helpers'

module Qless
  describe Workers::SerialWorker, :integration do
    include Qless::WorkerHelpers

    let(:key) { :worker_integration_job }
    let(:queue) { client.queues['main'] }
    let(:output) { StringIO.new }
    let(:worker) do
      Qless::Workers::SerialWorker.new(
        Qless::JobReservers::RoundRobin.new([queue]),
        interval: 1,
        max_startup_interval: 0,
        output: output,
        log_level: Logger::DEBUG)
    end

    it 'does not leak threads' do
      job_class = Class.new do
        def self.perform(job)
          Redis.connect(url: job['redis']).rpush(job['key'], job['word'])
        end
      end
      stub_const('JobClass', job_class)

      # Put in a single job
      queue.put('JobClass', { redis: redis.client.id, key: key, word: 'hello' })
      expect do
        run_jobs(worker, 1) do
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'hello'])
        end
      end.not_to change { Thread.list }
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
      run_jobs(worker, 3) do
        words.each do |word|
          redis.brpop(key, timeout: 1).should eq([key.to_s, word])
        end
      end
    end

    it 'does not keep `current_job` set at the last job when it is in a sleep loop' do
      job_class = Class.new do
        def self.perform(job)
          job.client.redis.rpush(job['key'], 'OK')
        end
      end
      stub_const('JobClass', job_class)
      queue.put('JobClass', { key: key })

      run_worker_concurrently_with(worker) do
        redis.brpop(key, timeout: 1).should eq([key.to_s, "OK"])
      end

      expect { |b| worker.send(:with_current_job, &b) }.to yield_with_args(nil)
    end

    context 'when a job times out', :uses_threads do
      it 'invokes the given callback when the current job is the one that timed out' do
        callback_invoked = false
        worker.on_current_job_lock_lost do
          callback_invoked = true
          Thread.main.raise(Workers::JobLockLost)
        end

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
              redis.rpush(job['key'], job['word'])
            end
          end
        end
        stub_const('JobClass', job_class)

        # Put this job into the queue and then busy-wait for the job to be
        # running, time it out, then make sure it eventually completes
        queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                  jid: 'jid')
        run_jobs(worker, 2) do
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'foo'])
          client.jobs['jid'].timeout
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'foo'])
          client.jobs['jid'].state.should eq('complete')
        end

        expect(callback_invoked).to be true
      end

      it 'does not invoke the given callback when a different job timed out' do
        callback_invoked = false
        worker.on_current_job_lock_lost { callback_invoked = true }

        # Job that sleeps for a while on the first pass
        job_class = Class.new do
          def self.perform(job)
            job.client.redis.rpush(job['key'], 'continue')
            sleep 2
          end
        end
        stub_const('JobClass', job_class)

        queue.put('JobClass', { key: key }, jid: 'jid1', priority: 100) # so it gets popped first
        queue.put('JobClass', { key: key }, jid: 'jid2', priority: 10)

        run_jobs(worker, 1) do
          expect(redis.brpop(key, timeout: 1)).to eq([key.to_s, 'continue'])
          job2 = queue.pop
          expect(job2.jid).to eq('jid2')
          job2.timeout
          Thread.main.raise("stop working")
        end

        expect(callback_invoked).to be false
      end

      it 'does not invoke the given callback when not running a job' do
        callback_invoked = false
        worker.on_current_job_lock_lost { callback_invoked = true }

        queue.put('JobClass', {})

        worker.listen_for_lost_lock do
          queue.pop.timeout
        end

        expect(callback_invoked).to be false
        # Subscriber logs errors to output; ensure there was no error
        expect(output.string).to eq("")
      end

      it 'does not blow up for jobs it does not have' do
        # Disable grace period
        client.config['grace-period'] = 0

        # A class that sends a message and sleeps for a bit
        job_class = Class.new do
          def self.perform(job)
            redis = Redis.connect(url: job['redis'])
            redis.rpush(job['key'], job['word'])
            redis.brpop(job['key'])
          end
        end
        stub_const('JobClass', job_class)

        # Put this job into the queue and then have the worker lose its lock
        queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                  priority: 10, jid: 'jid')
        queue.put('JobClass', { redis: redis.client.id, key: key, word: :foo },
                  priority: 5, jid: 'other')

        run_jobs(worker, 1) do
          # Busy-wait for the job to be running, and then time out another job
          redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
          job = queue.pop
          expect(job.jid).to eq('other')
          job.timeout
          # And the first should still be running
          client.jobs['jid'].state.should eq('running')
          redis.rpush(key, 'foo')
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
        run_jobs(worker, 1) do
          redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
          until client.jobs['jid'].state == 'waiting'; end
        end
        expect(client.jobs['jid'].retries_left).to be < 10
      end
    end
  end
end
