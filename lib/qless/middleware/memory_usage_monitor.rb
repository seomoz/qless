
module Qless
  module Middleware
    # Monitors the memory usage of the Qless worker, instructing
    # it to shutdown when memory exceeds the given :max_memory threshold.
    class MemoryUsageMonitor < Module
      def initialize(options)
        max_memory = options.fetch(:max_memory)

        module_eval do
          job_counter = 0

          define_method :around_perform do |job|
            job_counter += 1

            begin
              super(job)
            ensure
              current_mem = MemoryUsageMonitor.current_usage_in_kb
              if current_mem > max_memory
                log(:info, "Exiting after job #{job_counter} since current memory " \
                           "(#{current_mem} KB) has exceeded max allowed memory " \
                           "(#{max_memory} KB).")
                shutdown
              end
            end
          end
        end
      end

      SHELL_OUT_FOR_MEMORY = -> do
        # Taken from:
        # http://stackoverflow.com/a/4133642/29262
        Integer(`ps -o rss= -p #{Process.pid}`)
      end unless defined?(SHELL_OUT_FOR_MEMORY)

      begin
        require 'rusage'
      rescue LoadError
        warn "Could not load `rusage` gem. Falling back to shelling out "
             "to get process memory usage, which is several orders of magnitude slower."

        define_singleton_method(:current_usage_in_kb, &SHELL_OUT_FOR_MEMORY)
      else
        memory_ratio = Process.rusage.maxrss / SHELL_OUT_FOR_MEMORY.().to_f

        if (800...1200).cover?(memory_ratio)
          # OS X tends to return maxrss in Bytes.
          def self.current_usage_in_kb
            Process.rusage.maxrss / 1024
          end
        else
          # Linux tends to return maxrss in KB.
          def self.current_usage_in_kb
            Process.rusage.maxrss
          end
        end
      end
    end
  end
end

