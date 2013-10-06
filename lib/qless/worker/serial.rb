# Encoding: utf-8

# Qless requires
require 'qless'
require 'qless/worker/base'

module Qless
  module Workers
    # A worker that keeps popping off jobs and processing them
    class SerialWorker < BaseWorker
      def initialize(reserver, options = {})
        @allowed_memory_multiple = options.fetch(:allowed_memory_multiple) { 10 }
        @check_memory_interval   = options.fetch(:check_memory_interval)   { 10 }
        super(reserver, options)
      end

      def run
        log(:info, "Starting #{reserver.description} in #{Process.pid}")
        procline "Starting #{reserver.description}"
        register_signal_handlers

        reserver.prep_for_work!

        listen_for_lost_lock do
          jobs.each_with_index do |job, index|
            # Run the job we're working on
            log(:info, "Starting job #{job.klass_name} (#{job.jid} from #{job.queue_name})")

            # We want this set just before processing the first job, rather than before
            # the work loop, because there is a constant amount of memory needed by the
            # work loop (e.g. redis objects, etc) that we want taken into account
            # in the initial_memory
            @initial_memory ||= Qless.current_memory_usage_in_kb

            perform(job)
            log(:debug, "Finished job #{job.klass_name} (#{job.jid} from #{job.queue_name})")

            if too_much_memory?(index)
              @log.info("Exiting since current memory (#{Qless.current_memory_usage_in_kb} KB) " +
                        "has exceeded allowed multiple (#{@allowed_memory_multiple}) " +
                        "of original starting memory (#{@initial_memory} KB).")
              break
            end

            # So long as we're paused, we should wait
            while paused
              log(:debug, 'Paused...')
              sleep interval
            end
          end
        end
      end

    private

      def too_much_memory?(job_index)
        return false unless (job_index % @check_memory_interval).zero?

        current_mem = Qless.current_memory_usage_in_kb
        current_mem_multiple = current_mem / @initial_memory
        current_mem_multiple > @allowed_memory_multiple
      end
    end
  end
end
