require 'spec_helper'
require 'qless/middleware/requeue_exceptions'

module Qless
  module Middleware
    describe RequeueExceptions do
      let(:container_class) do
        Class.new do
          attr_accessor :perform

          def around_perform(job)
            perform.call
          end
        end
      end

      let(:container) { container_class.new }
      let(:job) { fire_double("Qless::Job", move: nil, queue_name: 'my-queue', data: {}) }
      let(:delay_range) { (0..30) }
      let(:max_attempts) { 20 }

      matched_exception_1 = ZeroDivisionError
      matched_exception_2 = KeyError
      unmatched_exception = RegexpError

      before do
        container.extend(RequeueExceptions)
        container.requeue_on matched_exception_1, matched_exception_2,
          delay_range: delay_range, max_attempts: max_attempts
      end

      def perform
        container.around_perform(job)
      end

      context 'when no exception is raised' do
        before { container.perform = -> { } }

        it 'does not requeue the job' do
          job.should_not_receive(:move)
          perform
        end
      end

      context 'when an unmatched exception is raised' do
        before { container.perform = -> { raise unmatched_exception } }

        it 'allows the error to propagate' do
          job.should_not_receive(:move)
          expect { perform }.to raise_error(unmatched_exception)
        end
      end

      [
        ["a matched exception", matched_exception_1, matched_exception_1.name],
        ["another matched exception", matched_exception_2, matched_exception_2.name],
        ["a subclass of a matched exception", Class.new(matched_exception_1), matched_exception_1.name],
      ].each do |description, exception, exception_name|
        context "when #{description} is raised" do
          before { container.perform = -> { raise exception } }

          it 'requeues the job' do
            job.should_receive(:move).with('my-queue', anything)
            perform
          end

          it 'uses a random delay from the delay_range' do
            Kernel.srand(100)
            sample = delay_range.to_a.sample

            job.should_receive(:move).with('my-queue', hash_including(delay: sample))

            Kernel.srand(100)
            perform
          end

          it 'tracks the number of requeues for this error' do
            expected_first_time = { 'requeues_by_exception' => { exception_name => 1 } }
            job.should_receive(:move).with('my-queue', hash_including(
              data: expected_first_time
            ))
            perform

            job.data.merge!(expected_first_time)

            job.should_receive(:move).with('my-queue', hash_including(
              data: { 'requeues_by_exception' => { exception_name => 2 } }
            ))
            perform
          end

          it 'preserves other requeues_by_exception values' do
            job.data['requeues_by_exception'] = { 'SomeKlass' => 3 }

            job.should_receive(:move).with('my-queue', hash_including(
              data: { 'requeues_by_exception' => { exception_name => 1, 'SomeKlass' => 3 } }
            ))
            perform
          end

          it 'preserves other data' do
            job.data['foo'] = 3

            job.should_receive(:move).with('my-queue', hash_including(
              data: { 'requeues_by_exception' => { exception_name => 1 }, 'foo' => 3 }
            ))
            perform
          end

          it 'allow the error to propogate when the max_attempts are exceeded' do
            job.data['requeues_by_exception'] = { exception_name => max_attempts }
            job.should_not_receive(:move)

            expect { perform }.to raise_error(exception)
          end
        end
      end
    end
  end
end

