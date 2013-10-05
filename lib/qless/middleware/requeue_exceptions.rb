# Encoding: utf-8

module Qless
  module Middleware
    # This middleware is like RetryExceptions, but it doesn't use qless-core's
    # internal retry/retry-tracking mechanism. Instead, it re-queues the job
    # when it fails with a matched error, and increments a counter in the job's
    # data.
    #
    # This is useful for exceptions for which you want a different
    # backoff/retry strategy. The internal retry mechanism doesn't allow for
    # separate tracking by exception type, and thus doesn't allow you to retry
    # different exceptions a different number of times.
    #
    # This is particularly useful for handling resource throttling errors,
    # where you may not want exponential backoff, and you may want the error
    # to be retried many times, w/o having other transient errors retried so
    # many times.
    module RequeueExceptions
      RequeueableException = Struct.new(:klass, :delay_range, :max_attempts) do
        def self.from_splat_and_options(*klasses, options)
          klasses.map do |klass|
            new(klass,
                options.fetch(:delay_range).to_a,
                options.fetch(:max_attempts))
          end
        end

        def delay
          delay_range.sample
        end

        def raise_if_exhausted_requeues(error, requeues)
          raise error if requeues >= max_attempts
        end
      end

      def requeue_on(*exceptions, options)
        RequeueableException.from_splat_and_options(
          *exceptions, options).each do |exc|
          requeueable_exceptions[exc.klass] = exc
        end
      end

      def around_perform(job)
        super
      rescue *requeueable_exceptions.keys => e
        config = requeuable_exception_for(e)

        requeues_by_exception = (job.data['requeues_by_exception'] ||= {})
        requeues_by_exception[config.klass.name] ||= 0

        config.raise_if_exhausted_requeues(
          e, requeues_by_exception[config.klass.name])

        requeues_by_exception[config.klass.name] += 1
        job.move(job.queue_name, delay: config.delay, data: job.data)
      end

      def requeueable_exceptions
        @requeueable_exceptions ||= {}
      end

      def requeuable_exception_for(e)
        requeueable_exceptions.fetch(e.class) do
          requeueable_exceptions.each do |klass, exc|
            break exc if klass === e
          end
        end
      end
    end
  end
end
