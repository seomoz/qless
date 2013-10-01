# Yield with a worker running, and then clean the worker up afterwards
module Qless
  module SubprocessWorkerRunner
    def in_subprocess_run(worker)
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
  end
end
