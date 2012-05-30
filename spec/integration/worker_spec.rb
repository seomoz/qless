require 'spec_helper'
require 'redis'
require 'yaml'
require 'qless/worker'
require 'qless'

class WorkerIntegrationJob
  def self.perform(job)
    Redis.connect(:url => job['redis_url']).rpush('worker_integration_job', job['word'])
  end
end

describe "Worker integration", :integration do
  def initialize_and_start_worker(run_as_single_process)
    if run_as_single_process
      unless @child = fork
        start_worker(run_as_single_process)
      end
    else
      start_worker(run_as_single_process)
    end
  end

  def start_worker(run_as_single_process)
    with_env_vars 'REDIS_URL' => redis_url, 'QUEUE' => 'main', 'INTERVAL' => '0.0001', 'RUN_AS_SINGLE_PROCESS ' => run_as_single_process do
      Qless::Worker.start
      exit!
    end
  end

  shared_examples_for 'a running worker' do |run_as_single_process|
    it 'can start a worker and then shut it down' do
      words = %w{foo bar howdy}
      initialize_and_start_worker(run_as_single_process)

      queue = client.queues["main"]
      words.each do |word|
        queue.put(WorkerIntegrationJob, "word" => word, "redis_url" => client.redis.client.id)
      end

      # Wait for the job to complete, and then kill the child process
      words.each do |word|
        client.redis.brpop('worker_integration_job', 10).should eq(['worker_integration_job', word])
      end
      Process.kill("QUIT", @child)
    end
  end

  it_behaves_like 'a running worker', '0'

  it_behaves_like 'a running worker', '1'
end

