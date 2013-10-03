# Encoding: utf-8

require 'raven'

module Qless
  module Middleware
    # This middleware logs errors to the sentry exception notification service:
    # http://getsentry.com/
    module Sentry
      def around_perform(job)
        super
      rescue Exception => e
        SentryLogger.new(e, job).log
        raise
      end

      # Logs a single exception to Sentry, adding pertinent job info.
      class SentryLogger
        def initialize(exception, job)
          @exception, @job = exception, job
        end

        def log
          event = ::Raven::Event.capture_exception(@exception) do |evt|
            evt.extra = { job: job_metadata }
          end

          safely_send event
        end

      private

        def safely_send(event)
          return unless event
          ::Raven.send(event)
        rescue
          # We don't want to silence our errors when the Sentry server
          # responds with an error. We'll still see the errors on the
          # Qless Web UI.
        end

        def job_metadata
          {
            jid:      @job.jid,
            klass:    @job.klass_name,
            history:  job_history,
            data:     @job.data,
            queue:    @job.queue_name,
            worker:   @job.worker_name,
            tags:     @job.tags,
            priority: @job.priority
          }
        end

        # We want to log formatted timestamps rather than integer timestamps
        def job_history
          @job.queue_history.map do |history_event|
            history_event.each_with_object({}) do |(key, value), hash|
              hash[key] = value.is_a?(Time) ? value.iso8601 : value
            end
          end
        end
      end
    end
  end
end
