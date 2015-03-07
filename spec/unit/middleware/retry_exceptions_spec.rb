# Encoding: utf-8

require 'spec_helper'
require 'qless/middleware/retry_exceptions'
require 'qless'

module Qless
  module Middleware
    describe RetryExceptions do
      let(:container_class) do
        Class.new do
          attr_accessor :perform

          def around_perform(job)
            perform.call
          end
        end
      end

      let(:container) { container_class.new }
      let(:job) do
        instance_double('Qless::Job', retry: nil, original_retries: 5,
                                      retries_left: 5, klass_name: 'JobClass')
      end
      let(:matched_exception) { ZeroDivisionError }
      let(:unmatched_exception) { RegexpError }

      before do
        container.extend(RetryExceptions)
        container.retry_on matched_exception
      end

      def perform
        container.around_perform(job)
      end

      def add_retry_callback
        container.use_on_retry_callback { |error, job| callback_catcher << [error, job] }
      end

      def callback_catcher
        @callback_catcher ||= []
      end

      describe '.use_on_retry_callback' do
        it 'uses a default callback if none is given' do
          expect(container.on_retry_callback).to eq(
            RetryExceptions::DEFAULT_ON_RETRY_CALLBACK)
        end

        it 'accepts a block to set an after retry callback' do
          container.use_on_retry_callback { |*| true }
          expect(container.on_retry_callback).not_to eq(
            RetryExceptions::DEFAULT_ON_RETRY_CALLBACK)
        end
      end

      context 'when no exception is raised' do
        before { container.perform = -> { } }

        it 'does not retry the job' do
          job.should_not_receive(:retry)
          perform
        end
      end

      context 'when an exception that does not match a named one is raised' do
        before { container.perform = -> { raise unmatched_exception } }

        it 'does not retry the job and allows the exception to propagate' do
          job.should_not_receive(:retry)
          expect { perform }.to raise_error(unmatched_exception)
        end

        it 'allows the exception to propagate' do
          expect { perform }.to raise_error(unmatched_exception)
        end

        context 'when an after retry callback is set' do
          before { add_retry_callback }

          it 'does not call the callback' do
            expect { perform }.to raise_error(unmatched_exception)

            expect(callback_catcher.size).to eq(0)
          end
        end
      end

      context 'when an exception that matches is raised' do
        let(:raise_line) { __LINE__ + 1 }
        before { container.perform = -> { raise matched_exception } }

        it 'retries the job, defaulting to no delay' do
          job.should_receive(:retry).with(0, anything, anything)
          perform
        end

        it 'passes along the failure details when retrying' do
          job.should_receive(:retry).with(
            anything,
            "JobClass:#{matched_exception.name}",
            /#{File.basename __FILE__}:#{raise_line}/)
          perform
        end

        it 'does not allow the exception to propagate' do
          expect { perform }.not_to raise_error
        end

        it 're-raises the exception if there are no retries left' do
          job.stub(retries_left: 0)
          expect { perform }.to raise_error(matched_exception)
        end

        it 're-raises the exception if there are negative retries left' do
          job.stub(retries_left: -1)
          expect { perform }.to raise_error(matched_exception)
        end

        context 'when an after retry callback is set' do
          before { add_retry_callback }

          it 'calls the callback' do
            expect {
              perform
            }.to change { callback_catcher.size }.from(0).to(1)
          end
        end

        def perform_and_track_delays
          delays = []
          job.stub(:retry) { |delay| delays << delay }

          5.downto(1) do |i|
            job.stub(retries_left: i)
            perform
          end

          delays
        end

        context 'with a lambda backoff retry strategy' do
          it 'uses the value returned by the lambda as the delay' do
            container.use_backoff_strategy { |num| num * 2 }
            delays = perform_and_track_delays
            expect(delays).to eq([2, 4, 6, 8, 10])
          end

          it 'passes the exception to the block so it can use it as part of the logic' do
            container.use_backoff_strategy do |num, error|
              expect(error).to be_a(matched_exception)
              num * 3
            end

            delays = perform_and_track_delays

            expect(delays).to eq([3, 6, 9, 12, 15])
          end
        end

        context 'with an exponential backoff retry strategy' do
          it 'generates an exponential delay' do
            container.instance_eval do
              use_backoff_strategy exponential(10)
            end

            delays = perform_and_track_delays

            expect(delays).to eq([10, 100, 1_000, 10_000, 100_000])
          end

          it 'generates an exponential delay using explicitly given factor' do
            container.instance_eval do
              use_backoff_strategy exponential(10, factor: 3)
            end

            delays = perform_and_track_delays

            expect(delays).to eq([10, 30, 90, 270, 810])
          end

          it 'when fuzz_factor given, dissipate delays over range' do
            container.instance_eval do
              use_backoff_strategy exponential(10, fuzz_factor: 0.3)
            end

            delays = perform_and_track_delays

            [10, 100, 1_000, 10_000, 100_000].zip(delays).each do |unfuzzed, actual|
              expect(actual).not_to eq(unfuzzed)
              expect(actual).to be_within(30).percent_of(unfuzzed)
            end
          end

          it 'combines factor and fuzz_factor' do
            container.instance_eval do
              use_backoff_strategy exponential(100, factor: 2, fuzz_factor: 0.2)
            end

            delays = perform_and_track_delays

            [100, 200, 400, 800, 1600].zip(delays).each do |unfuzzed, actual|
              expect(actual).not_to eq(unfuzzed)
              expect(actual).to be_within(20).percent_of(unfuzzed)
            end
          end

          it 'can be reused by multiple jobs' do
            container.instance_eval do
              use_backoff_strategy exponential(10, factor: 2)
            end
            perform_and_track_delays

            delays = perform_and_track_delays

            expect(delays).to eq([10, 20, 40, 80, 160])
          end
        end
      end
    end
  end
end
