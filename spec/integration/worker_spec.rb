require 'spec_helper'
require 'redis'
require 'yaml'
require 'qless/worker'
require 'qless'
require 'qless/middleware/retry_exceptions'

# A job that just puts a word in a redis list to show that its done
class WorkerIntegrationJob
  def self.perform(job)
    redis = Redis.connect(url: job['redis_url'])
    redis.rpush(job['key'], job['word'])
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
  # Yield with a worker running, and then clean the worker up afterwards
  def with_worker
    @child = fork do
      with_env_vars 'REDIS_URL' => redis_url, 'QUEUE' => 'main' do
        Qless::Worker.start
      end
    end

    begin
      yield
    ensure
      Process.kill('TERM', @child)
      Process.wait(@child)
    end
  end

  it 'can start a worker and then shut it down' do
    words = %w{foo bar howdy}
    key = :worker_integration_job

    queue = client.queues["main"]
    words.each do |word|
      queue.put(WorkerIntegrationJob, {
        redis_url: client.redis.client.id, word: word, key: key})
    end

    # Wait for the job to complete, and then kill the child process
    with_worker do
      words.each do |word|
        client.redis.brpop(key, timeout: 1).should eq([key.to_s, word])
      end
    end
  end

  it 'does not blow up when the child process exits unexpectedly' do
    key = :worker_integration_job

    job_class = Class.new do
      def self.perform(job)
        # Fall on our sword so long as we have retries left
        if job.retries_left > 1
          Process.kill(9, Process.pid)
        else
          redis = Redis.connect(url: job['redis_url'])
          redis.rpush(job['key'], job['word'])
        end
      end
    end
    stub_const("SuicidalJob", job_class)

    client.config['grace-period'] = 0
    queue = client.queues['main']
    queue.heartbeat = -100
    queue.put(SuicidalJob, {
        redis_url: client.redis.client.id, word: :foo, key: key}, retries: 5)

    with_worker do
      client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
    end
  end

  it 'will retry and eventually fail a repeatedly failing job' do
    queue = client.queues["main"]
    jid = queue.put(RetryIntegrationJob, {"redis_url" => client.redis.client.id}, retries: 10)
    Qless::Worker.new(
      Qless::JobReservers::RoundRobin.new([queue])
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
        pending('This needs some work')
        jid = enqueue_job_and_process(BroadcastLockLostForDifferentJIDJob)
        expect(client.redis.get("broadcast_lock_lost_job_completed")).to eq("true")

        job = client.jobs[jid]
        expect(job.state).to eq("complete")
      end
    end

    it 'takes a new job' do
      pending('This needs some work')
    end

    it 'fails the job with an error containing the job backtrace' do
      pending('This needs some work')
      jid = enqueue_job_and_process

      job = client.jobs[jid]
      expect(job.state).to eq("failed")
      expect(job.failure.fetch('group')).to eq("SlowJob:Qless::Worker::JobLockLost")
      expect(job.failure.fetch('message')).to include("worker_spec.rb:#{slow_job_line}")
    end
  end
end
