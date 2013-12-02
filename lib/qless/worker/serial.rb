# Encoding: utf-8

# Qless requires
require 'qless'
require 'qless/worker/base'

module Qless
  module Workers
    # A worker that keeps popping off jobs and processing them
    class SerialWorker < BaseWorker
      def initialize(reserver, options = {})
        super(reserver, options)
      end

      def run
        log(:info, "Starting #{reserver.description} in #{Process.pid}")
        procline "Starting #{reserver.description}"
        register_signal_handlers

        reserver.prep_for_work!

        listen_for_lost_lock do
          procline "Running #{reserver.description}"

          jobs.each do |job|
            # Run the job we're working on
            log(:debug, "Starting job #{job.klass_name} (#{job.jid} from #{job.queue_name})")
            perform(job)
            log(:debug, "Finished job #{job.klass_name} (#{job.jid} from #{job.queue_name})")

            # So long as we're paused, we should wait
            while paused
              log(:debug, 'Paused...')
              sleep interval
            end
          end
        end
      end
    end
  end
end
