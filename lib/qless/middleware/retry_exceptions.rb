# Encoding: utf-8

module Qless
  module Middleware
    # Auto-retries particular errors using qless-core's internal retry tracking
    # mechanism. Supports a backoff strategy (typically exponential).
    #
    # Note: this does not support varying the number of allowed retries by
    # exception type. If you want that kind of flexibility, use the
    # RequeueExceptions middleware instead.
    module RetryExceptions
      def around_perform(job)
        super
      rescue *retryable_exception_classes => error
        raise if job.retries_left <= 0

        attempt_num = (job.original_retries - job.retries_left) + 1
        failure = Qless.failure_formatter.format(job, error)
        job.retry(backoff_strategy.call(attempt_num, error), *failure)

        on_retry_callback.call(error, job)
      end

      def retryable_exception_classes
        @retryable_exception_classes ||= []
      end

      def retry_on(*exception_classes)
        retryable_exception_classes.push(*exception_classes)
      end

      NO_BACKOFF_STRATEGY = ->(_num, _error) { 0 }

      def use_backoff_strategy(strategy = nil, &block)
        @backoff_strategy = strategy || block
      end

      def backoff_strategy
        @backoff_strategy ||= NO_BACKOFF_STRATEGY
      end

      DEFAULT_ON_RETRY_CALLBACK = lambda { |error, job| }
      def use_on_retry_callback(&block)
        @on_retry_callback = block if block
      end

      def on_retry_callback
        @on_retry_callback ||= DEFAULT_ON_RETRY_CALLBACK
      end

      # If `factor` is omitted it is set to `delay_seconds` to reproduce legacy
      # behavior.
      def exponential(delay_seconds, options={})
        factor = options.fetch(:factor, delay_seconds)
        fuzz_factor = options.fetch(:fuzz_factor, 0)

        lambda do |retry_no, error|
          unfuzzed = delay_seconds * factor**(retry_no - 1)
          return unfuzzed if fuzz_factor.zero?
          r = 2 * rand  - 1
          # r is uniformly distributed in range [-1, 1]
          unfuzzed * (1 + fuzz_factor * r)
        end
      end
    end
  end

  # For backwards compatibility
  module RetryExceptions
    include Middleware::RetryExceptions
  end
end
