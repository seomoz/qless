module Qless
  module Middleware
    module RetryExceptions
      def around_perform(job)
        super
      rescue *retryable_exception_classes => e
        raise if job.retries_left <= 0

        attempt_num = (job.original_retries - job.retries_left) + 1
        job.retry(backoff_strategy.call(attempt_num))
      end

      def retryable_exception_classes
        @retryable_exception_classes ||= []
      end

      def retry_on(*exception_classes)
        retryable_exception_classes.push(*exception_classes)
      end

      NO_BACKOFF_STRATEGY = lambda { |num| 0 }

      def use_backoff_strategy(strategy = nil, &block)
        @backoff_strategy = strategy || block
      end

      def backoff_strategy
        @backoff_strategy ||= NO_BACKOFF_STRATEGY
      end

      def exponential(base, options = {})
        rand_fuzz = options.fetch(:rand_fuzz, 1)
        lambda do |num|
          base ** num + rand(rand_fuzz)
        end
      end
    end
  end

  # For backwards compatibility
  RetryExceptions = Middleware::RetryExceptions
end

