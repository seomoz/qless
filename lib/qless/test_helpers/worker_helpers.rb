module Qless
  module WorkerHelpers
    # Yield with a worker running, and then clean the worker up afterwards
    def run_worker_concurrently_with(worker, &block)
      thread = Thread.start { stop_worker_after(worker, &block) }
      thread.abort_on_exception = true
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
      worker.extend Module.new {
        define_method(:jobs) do
          base_enum = super()
          Enumerator.new do |enum|
            count.times { enum << base_enum.next }
          end
        end
      }

      thread = Thread.start { yield } if block_given?
      thread.abort_on_exception if thread
      worker.run
    ensure
      thread.join(0.1) if thread
    end

    # Runs the worker until it has no more jobs to process,
    # effectively drainig its queues.
    def drain_worker_queues(worker)
      worker.extend Module.new {
        # For the child: stop as soon as it can't pop more jobs.
        def no_job_available
          shutdown
        end

        # For the parent: when the child stops,
        # don't try to restart it; shutdown instead.
        def spawn_replacement_child(*)
          shutdown
        end
      }

      worker.run
    end
  end
end
