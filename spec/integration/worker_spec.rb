require 'spec_helper'
require 'redis'
require 'yaml'
require 'qless/worker'
require 'qless'

class WorkerIntegrationJob
  def self.perform(job)
    File.open(job['file'], "w") { |f| f.write("done") }
  end
end

describe "Worker integration", :integration do
  def start_worker
    unless @child = fork
      with_env_vars 'REDIS_URL' => redis_url, 'QUEUE' => 'main', 'INTERVAL' => '0.0001' do
        Qless::Worker.start
        exit! # once the work ends, exit hte worker process
      end
    end
  end

  def output_file(i)
    File.join(temp_dir, "job.out.#{Time.now.to_i}.#{i}")
  end

  def wait_for_job_completion
    20.times do |i|
      sleep 0.1
      return if `ps -p #{@child}` =~ /Waiting/i
    end

    raise "Didn't complete: #{`ps -p #{@child}`}"
  end

  let(:temp_dir) { "./spec/tmp" }

  before do
    FileUtils.rm_rf temp_dir
    FileUtils.mkdir_p temp_dir
  end

  it 'can start a worker and then shut it down' do
    files = 3.times.map { |i| output_file(i) }
    files.select { |f| File.exist?(f) }.should eq([])

    start_worker

    queue = client.queue("main")
    files.each do |f|
      queue.put(WorkerIntegrationJob, "file" => f)
    end

    wait_for_job_completion
    Process.kill("QUIT", @child) # send shutdown signal

    contents = files.map { |f| File.read(f) }
    contents.should eq(['done'] * files.size)
  end
end

