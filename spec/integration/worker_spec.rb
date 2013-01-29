require 'spec_helper'
require 'redis'
require 'yaml'
require 'qless/worker'
require 'qless'
require 'qless/retry_exceptions'

class WorkerIntegrationJob
  def self.perform(job)
    Redis.connect(url: job['redis_url']).rpush('worker_integration_job', job['word'])
  end
end

class RetryIntegrationJob
  extend Qless::RetryExceptions

  Kaboom = Class.new(StandardError)
  retry_on Kaboom

  def self.perform(job)
    Redis.connect(url: job['redis_url']).incr('retry_integration_job_count')
    raise Kaboom
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

  it 'will retry and eventually fail a repeatedly failing job' do
    queue = client.queues["main"]
    jid = queue.put(RetryIntegrationJob, {"redis_url" => client.redis.client.id}, retries: 10)
    Qless::Worker.new(
      Qless::JobReservers::RoundRobin.new([queue]),
      run_as_a_single_process: true
    ).work(0)

    job = client.jobs[jid]

    job.state.should eq('failed')
    job.retries_left.should eq(-1)
    job.original_retries.should eq(10)
    client.redis.get('retry_integration_job_count').should eq('11')
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

