# Yield with a worker running, and then clean the worker up afterwards
def fork_worker(worker)
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

def thread_worker(worker)
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

# Yield with a worker running in a thread, run only count jobs clean up after
def run_jobs(worker, count = nil)
  thread = Thread.new do
    unless count.nil?
      jobs = worker.jobs
      worker.stub(:jobs) do 
        Enumerator.new do |enum|
          count.times do
            enum.yield(jobs.next)
          end
        end
      end
    end
    worker.run
  end

  begin
    yield
  ensure
    thread.join
  end
end