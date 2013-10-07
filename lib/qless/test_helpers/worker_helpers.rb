module Qless
  module WorkerHelpers
    # Yield with a worker running, and then clean the worker up afterwards
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

    # Run only the given number of jobs, then stop
    def run_jobs(worker, count)
      worker.job_limit = count
      thread = Thread.start { yield } if block_given?
      worker.run
    ensure
      thread.join(0.1) if thread
    end
  end
end
