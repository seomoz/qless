
module Qless
  module Middleware
    class MemoryUsageMonitor < Module

      begin
        require 'proc/wait3'
        def self.current_usage
          Process.getrusage.maxrss
        end
      rescue LoadError
        warn "Could not load `proc-wait3` gem. Falling back to shelling out to get process memory usage, " +
             "which is several orders of magnitude slower."

        def self.current_usage
          # Taken from:
          # http://stackoverflow.com/a/4133642/29262
          Integer(`ps -o rss= -p #{Process.pid}`) * 1024
        end
      end
    end
  end
end

