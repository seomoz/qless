module Qless
  module RetryExceptions
    def retryable_exception_classes
      @retryable_exception_classes ||= []
    end

    def retry_on(*exception_classes)
      self.retryable_exception_classes.push(*exception_classes)
    end
  end
end
