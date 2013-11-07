
module Qless
  module Middleware
    # Monitors the memory usage of the Qless worker, instructing
    # it to shutdown when memory exceeds the given :max_memory threshold.
    class MemoryUsageMonitor < Module
      def initialize(options)
        max_memory = options.fetch(:max_memory)

        module_eval do
          define_method :around_perform do |job|
            super(job)

            current_mem = MemoryUsageMonitor.current_usage
            if current_mem > max_memory
              log(:info, "Exiting since current memory (#{format_large_number current_mem} B) " +
                         "has exceeded max allowed memory (#{format_large_number max_memory} B).")
              shutdown
            end
          end

          def format_large_number(num)
            # From stack overflow:
            # http://stackoverflow.com/a/6460147/29262
            num.to_s.gsub(/(?<=\d)(?=(?:\d{3})+\z)/, ',')
          end
        end
      end

      begin
        require 'rusage'
        def self.current_usage
          Process.rusage.maxrss
        end
      rescue LoadError
        warn "Could not load `rusage` gem. Falling back to shelling out to get process memory usage, " +
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

