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
  def start_worker
    unless @child = fork
      with_env_vars 'REDIS_URL' => redis_url, 'QUEUE' => 'main', 'INTERVAL' => '0.0001' do
        Qless::Worker.start
        exit!
      end
    end
  end
  
  it 'can start a worker and then shut it down' do
    words = %w{foo bar howdy}
    start_worker
    
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

