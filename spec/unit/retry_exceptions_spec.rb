require 'qless/retry_exceptions'

module Qless
  describe RetryExceptions do
    let(:job_class) { Class.new }
    let(:exception_class) { Class.new(StandardError) }

    before do
      job_class.extend(RetryExceptions)
    end

    it 'defines a retryable_exceptions method that returns an empty array by default' do
      job_class.retryable_exception_classes.should be_empty
    end

    it 'defines a retry_on method that makes exception types retryable' do
      job_class.retry_on(exception_class)

      job_class.retryable_exception_classes.should eq([exception_class])
    end
  end
end
