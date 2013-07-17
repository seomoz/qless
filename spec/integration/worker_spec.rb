require 'spec_helper'
require 'redis'
require 'yaml'
require 'qless/worker'
require 'qless'
require 'qless/middleware/retry_exceptions'

class WorkerIntegrationJob
  def self.perform(job)
    Redis.connect(url: job['redis_url']).rpush('worker_integration_job', job['word'])
  end
end

class RetryIntegrationJob
  extend Qless::Job::SupportsMiddleware
  extend Qless::Middleware::RetryExceptions

  Kaboom = Class.new(StandardError)
  retry_on Kaboom

  def self.perform(job)
    Redis.connect(url: job['redis_url']).incr('retry_integration_job_count')
    raise Kaboom
  end
end

class SlowJob
  def self.perform(job)
    sleep 1.1
    qless = Qless::Client.new(url: job['redis_url'])
    qless.queues["main"].pop # to trigger the lua script to timeout the job
    sleep 3
    qless.redis.set("slow_job_completed", "true")
  end
end
slow_job_line = __LINE__ - 4

class BroadcastLockLostForDifferentJIDJob
  def self.perform(job)
    worker_name = job['worker_name']
    redis = Redis.connect(url: job['redis_url'])
    message = JSON.dump(jid: 'abc', event: 'lock_lost', worker: worker_name)
    listener_count = redis.publish("ql:w:#{worker_name}", message)
    raise "Worker is not listening, apparently" unless listener_count == 1

    sleep 1
    redis.set("broadcast_lock_lost_job_completed", "true")
  end
end

describe "Worker integration", :integration do
  def start_worker(run_as_single_process)
    unless @child = fork
      with_env_vars 'REDIS_URL' => redis_url, 'QUEUE' => 'main', 'INTERVAL' => '0.0001', 'RUN_AS_SINGLE_PROCESS' => run_as_single_process do
        Qless::Worker.start
        exit!
      end
    end
  end

  shared_examples_for 'a running worker' do |run_as_single_process|
    it 'can start a worker and then shut it down' do
      words = %w{foo bar howdy}
      start_worker(run_as_single_process)

      queue = client.queues["main"]
      words.each do |word|
        queue.put(WorkerIntegrationJob, "word" => word, "redis_url" => client.redis.client.id)
      end

      # Wait for the job to complete, and then kill the child process
      words.each do |word|
        client.redis.brpop('worker_integration_job', 10).should eq(['worker_integration_job', word])
      end
    end

    after(:each) do
      @child && Process.kill("QUIT", @child)
    end
  end

  it_behaves_like 'a running worker'

  it_behaves_like 'a running worker', '1'

  it 'does not blow up when the child process exits unexpectedly' do
    job_class = Class.new do
      def self.perform(job)
        Process.kill(9, Process.pid)
      end
    end

    stub_const("SuicidalJob", job_class)

    queue = client.queues["main"]
    jid = queue.put(SuicidalJob, {})

    Qless::Worker.new(Qless::JobReservers::RoundRobin.new([queue])).work(0)
    expect(client.jobs[jid].state).to eq("running")
  end

  it 'will retry and eventually fail a repeatedly failing job' do
    queue = client.queues["main"]
    jid = queue.put(RetryIntegrationJob, {"redis_url" => client.redis.client.id}, retries: 10)
    Qless::Worker.new(
      Qless::JobReservers::RoundRobin.new([queue]),
      run_as_a_single_process: true
    ).work(0)

    job = client.jobs[jid]

    job.state.should eq('failed')
    job.retries_left.should eq(0)
    job.original_retries.should eq(10)
    client.redis.get('retry_integration_job_count').should eq('11')
  end

  it 'does not leak threads' do
    queue = client.queues["main"]
    queue.put(WorkerIntegrationJob, "word" => "foo", "redis_url" => client.redis.client.id)

    expect {
      Qless::Worker.new(Qless::JobReservers::RoundRobin.new([queue])).work(0)
    }.not_to change { Thread.list }
  end

  context 'when a job times out' do
    include_context "stops all non-main threads"
    let(:queue) { client.queues["main"] }
    let(:worker) { Qless::Worker.new(Qless::JobReservers::RoundRobin.new([queue])) }

    def enqueue_job_and_process(klass = SlowJob)
      queue.heartbeat = 1

      jid = queue.put(klass, "redis_url" => client.redis.client.id,
                      "worker_name" => Qless.worker_name)
      worker.work(0)
      jid
    end

    context 'when the lock_list message has the jid of a different job' do
      it 'does not kill or fail the job' do
        jid = enqueue_job_and_process(BroadcastLockLostForDifferentJIDJob)
        expect(client.redis.get("broadcast_lock_lost_job_completed")).to eq("true")

        job = client.jobs[jid]
        expect(job.state).to eq("complete")
      end
    end

    it 'kills the child process' do
      enqueue_job_and_process
      expect(client.redis.get("slow_job_completed")).to be_nil
    end

    it 'fails the job with an error containing the job backtrace' do
      jid = enqueue_job_and_process

      job = client.jobs[jid]
      expect(job.state).to eq("failed")
      expect(job.failure.fetch('group')).to eq("SlowJob:Qless::Worker::JobLockLost")
      expect(job.failure.fetch('message')).to include("worker_spec.rb:#{slow_job_line}")
    end

    it 'gracefully handles it if the child process is not listening to know to return the backtrace' do
      worker.stub(:start_child_pub_sub_listener_for) # so the child is not listening
      jid = enqueue_job_and_process

      job = client.jobs[jid]
      expect(job.state).to eq("failed")
      expect(job.failure.fetch('message')).to include("Could not obtain child backtrace")
    end

    it 'gracefully handles it if the child process dies before returning a backtrace' do
      stub_const("Qless::Worker::WAIT_FOR_CHILD_BACKTRACE_TIMEOUT", 1)
      worker.stub(:notify_parent_of_job_backtrace)
      jid = enqueue_job_and_process

      job = client.jobs[jid]
      expect(job.state).to eq("failed")
      expect(job.failure.fetch('message')).to include("Could not obtain child backtrace")
    end

    it 'ensures that the backtrace state is not left in redis if the parent dies before popping it' do
      stub_const("Qless::Worker::BACKTRACE_EXPIRATION_TIMEOUT_MS", 1)
      ::Redis.any_instance.stub(:blpop)
      enqueue_job_and_process
      expect(client.redis.keys("ql:child_backtraces*")).to eq([])
    end
  end
end

describe "when the child process is using the redis connection", :integration do
  class NotReconnectingJob
    def self.perform(job)
      # cheat and grab the redis object
      redis = job.instance_variable_get(:@client).redis

      # force a call to redis
      redis.info
    end
  end

  it 'does not raise an error' do
    queue = client.queues["main"]
    jid = queue.put(NotReconnectingJob, {})
    Qless::Worker.new(Qless::JobReservers::RoundRobin.new([queue]),
      run_as_a_single_process: false
    ).work(0)

    client.jobs[jid].state.should eq('complete')
  end
end

