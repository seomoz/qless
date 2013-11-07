
module Qless
  module Middleware
    class MemoryUsageMonitor < Module
      def initialize(options)
        initial_memory = nil
        allowed_memory_multiple = options.fetch(:allowed_memory_multiple) { 10 }

        module_eval do
          define_method :around_perform do |job|
            # We want this set just before processing the first job, rather than before
            # the work loop, because there is a constant amount of memory needed by the
            # work loop (e.g. redis objects, etc) that we want taken into account
            # in the initial_memory
            initial_memory ||= MemoryUsageMonitor.current_usage

            super(job)

            current_mem = MemoryUsageMonitor.current_usage
            current_mem_multiple = current_mem / initial_memory

            if current_mem_multiple > allowed_memory_multiple
              log(:info, "Exiting since current memory (#{format_large_number current_mem} B) " +
                         "has exceeded allowed multiple (#{format_large_number allowed_memory_multiple}) " +
                         "of original starting memory (#{format_large_number initial_memory} B).")
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

