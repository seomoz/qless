module Qless
  module RetryExceptions
    attr_accessor :retryable_exception_classes

    def self.extended(base)
      base.retryable_exception_classes = []
    end

    def retryable_exception?(exception)
      self.retryable_exception_classes.any? { |klass| exception.is_a?(klass) }
    end

    def retry_on(*exception_classes)
      self.retryable_exception_classes.push(*exception_classes)
    end
  end
end
