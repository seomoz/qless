# Encoding: utf-8

require 'spec_helper'
require 'redis'
require 'yaml'
require 'qless/worker'
require 'qless'
require 'qless/middleware/retry_exceptions'

# Yield with a worker running, and then clean the worker up afterwards
def with_worker
  child = fork do
    vars = {
      'REDIS_URL' => redis_url,
      'QUEUE' => 'main',
      'INTERVAL' => '1',
      'MAX_STARTUP_INTERVAL' => '0'
    }
    with_env_vars vars do
      Qless::Worker.start
    end
  end

  begin
    yield
  ensure
    Process.kill('TERM', child)
    Process.wait(child)
  end
end

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
    qless.queues['main'].pop # to trigger the lua script to timeout the job
    sleep 3
    qless.redis.set('slow_job_completed', 'true')
  end
end

class BroadcastLockLostForDifferentJIDJob
  def self.perform(job)
    worker_name = job['worker_name']
    redis = Redis.connect(url: job['redis_url'])
    message = JSON.dump(jid: 'abc', event: 'lock_lost', worker: worker_name)
    listener_count = redis.publish("ql:w:#{worker_name}", message)
    raise 'Worker is not listening, apparently' unless listener_count == 1

    sleep 1
    redis.set('broadcast_lock_lost_job_completed', 'true')
  end
end

describe 'Worker integration', :integration do
  it 'can start a worker and then shut it down' do
    words = %w{foo bar howdy}
    key = :worker_integration_job

    queue = client.queues['main']
    words.each do |word|
      queue.put(
        WorkerIntegrationJob,
        { redis_url: client.redis.client.id, word: word, key: key })
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
    stub_const('SuicidalJob', job_class)

    client.config['grace-period'] = 0
    queue = client.queues['main']
    queue.heartbeat = -100
    queue.put(
      SuicidalJob,
      { redis_url: client.redis.client.id, word: :foo, key: key }, retries: 5)

    with_worker do
      client.redis.brpop(key, timeout: 1).should eq([key.to_s, 'foo'])
    end
  end

  it 'will retry and eventually fail a repeatedly failing job' do
    queue = client.queues['main']
    jid = queue.put(RetryIntegrationJob,
                    { redis_url: client.redis.client.id }, retries: 10)
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
    queue = client.queues['main']
    queue.put(WorkerIntegrationJob,
              word: 'foo', redis_url: client.redis.client.id)

    expect do
      Qless::Worker.new(Qless::JobReservers::RoundRobin.new([queue])).work(0)
    end.not_to change { Thread.list }
  end

  context 'when a job times out' do
    include_context 'stops all non-main threads'
    let(:queue) { client.queues['main'] }
    let(:worker) do
      Qless::Worker.new(Qless::JobReservers::RoundRobin.new([queue]))
    end

    it 'takes a new job' do
      job_class = Class.new do
        def self.perform(job)
          # We'll sleep a bit before completing it the first time
          if job.raw_queue_history.length == 2
            sleep 1
            job.fail('foo', 'bar')
          else
            job.complete
          end
        end
      end
      stub_const('JobClass', job_class)

      # Put this job into the queue and then have the worker lose its lock
      jid = queue.put(JobClass, {}, retries: 5)
      client.config['grace-period'] = 0

      with_worker do
        # Busy-wait for the job to be running, then time out the job and wait
        # for it to complete
        while client.jobs[jid].state != 'running'; end
        client.jobs[jid].timeout
        while %w{stalled waiting running}.include?(client.jobs[jid].state); end
        client.jobs[jid].state.should eq('complete')
      end
    end

    it 'does not blow up for jobs it does not have' do
      job_class = Class.new do
        def self.perform(job)
          # We'll sleep a bit before completing it the first time
          sleep 10 if job.retries_left == 5
        end
      end
      stub_const('JobClass', job_class)

      # Put this job into the queue and then have the worker lose its lock
      first = queue.put(JobClass, {}, retries: 5)
      queue.put(JobClass, {}, retries: 5)
      client.config['grace-period'] = 0

      with_worker do
        # Busy-wait for the job to be running, and then time out another job
        while client.jobs[first].state != 'running'; end
        queue.pop.timeout
        # And the first should still be running
        client.jobs[first].state.should eq('running')
      end
    end

    it 'fails the job with an error containing the job backtrace' do
      pending('I do not think this is actually the desired behavior')
    end
  end
end
