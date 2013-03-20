module Qless
  module Middleware
    module RetryExceptions
      def around_perform(job)
        super
      rescue *retryable_exception_classes => e
        job.retry
      end

      def retryable_exception_classes
        @retryable_exception_classes ||= []
      end

      def retry_on(*exception_classes)
        retryable_exception_classes.push(*exception_classes)
      end
    end
  end

  # For backwards compatibility
  RetryExceptions = Middleware::RetryExceptions
end

