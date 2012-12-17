require 'qless/retry_exceptions'

module Qless
  describe RetryExceptions do
    let(:job_class) { Class.new }
    let(:exception_class) { Class.new(StandardError) }

    before do
      job_class.extend(RetryExceptions)
    end

    it 'defines a retryable_exception? method that returns false by default' do
      job_class.retryable_exception?(exception_class.new).should be_false
    end

    describe 'retry_on' do
      it 'defines a retry_on method that makes exception types retryable' do
        job_class.retry_on(exception_class)
        job_class.retryable_exception?(exception_class.new).should be_true
      end

      it 'makes subclasses retryable' do
        subclass = Class.new(exception_class)

        job_class.retry_on(exception_class)
        job_class.retryable_exception?(subclass.new).should be_true
      end
    end
  end
end
