module Qless
  module WorkerHelpers
    # Yield with a worker running, and then clean the worker up afterwards
    def run_worker_concurrently_with(worker, &block)
      thread = Thread.start { stop_worker_after(worker, &block) }
      worker.run
    ensure
      thread.join(0.1)
    end

    def stop_worker_after(worker, &block)
      yield
    ensure
      worker.stop!
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
