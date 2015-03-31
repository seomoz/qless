require 'spec_helper'
require 'support/forking_worker_context'
require 'qless/middleware/timeout'

module Qless::Middleware
  describe Timeout do
    include_context "forking worker"

    def define_job_class(name, &block)
      stub_const(name, Class.new(&block))
    end

    def duration_of
      start = Time.now
      yield
      Time.now - start
    end

    before do
      define_job_class "MyJobClass" do
        extend Qless::Job::SupportsMiddleware

        def self.perform(job)
          job.client.redis.rpush("in_job", "about_to_sleep")
          do_work
        end

        def self.do_work
          sleep
        end
      end
    end
    let(:sleep_line) { __LINE__ - 4 }

    def expect_job_to_timeout
      jid = queue.put MyJobClass, {}

      duration_of { drain_worker_queues(worker) }.tap do
        expect(redis.brpop("in_job", timeout: 1).last).to eq("about_to_sleep")
        job = client.jobs[jid]

        expect(job.failure["group"]).to include("JobTimedoutError")
        expect(job.failure["message"]).to include("do_work", "#{File.basename(__FILE__)}:#{sleep_line}")
        expect(log_io.string).to include("died with 73")
      end
    end

    it 'fails the job and kills the worker running it when it exceeds the provided timeout value' do
      MyJobClass.extend Qless::Middleware::Timeout.new { 0.05 }

      duration = expect_job_to_timeout
      expect(duration).to be < 0.2
    end

    it "can be applied to a worker rather than an individual job, which can use the job's TTL as a basis for the timeout value" do
      queue.heartbeat = 0.05
      worker.extend Qless::Middleware::Timeout.new { |job| job.ttl + 0.05 }

      duration = expect_job_to_timeout
      expect(duration).to be_between(0.1, 0.2)
    end

    it 'aborts with a clear error when given a non-positive timeout' do
      MyJobClass.extend Qless::Middleware::Timeout.new { 0 }

      jid = queue.put MyJobClass, {}
      drain_worker_queues(worker)
      job = client.jobs[jid]

      expect(job.failure["group"]).to include("InvalidTimeoutError")
    end
  end
end
