require 'spec_helper'
require 'qless/job'
require 'qless/middleware/timeout'

module Qless
  module Middleware
    ::RSpec.describe Timeout do
      class JobClass
        def around_perform_call_counter
          @around_perform_call_counter ||= 0
        end

        def around_perform(job)
          @around_perform_call_counter = (@around_perform_call_counter || 0) + 1
        end
      end

      let(:kernel_class) { class_double(Kernel).as_null_object }

      let(:job) { instance_double(Qless::Job).as_null_object }

      def make_worker(timeout_class, timeout_seconds, kernel_class)
        Class.new(JobClass) do
          include Qless::Middleware::Timeout.new(timeout_class: timeout_class,
              kernel_class: kernel_class) { timeout_seconds }
        end.new
      end

      class TriggeredTimeout
        def self.timeout(timeout_seconds)
          raise ::Timeout::Error.new("triggered at #{timeout_seconds}s")
        end
      end

      class InactiveTimeout
        def self.timeout(timeout_seconds)
          yield
        end
      end

      context 'when timeout specified as nil (to block timeout processing for specific jobs)' do
        it 'allows nil' do
          worker = make_worker(TriggeredTimeout, nil, kernel_class)

          expect {
            worker.around_perform job
          }.not_to raise_error
        end

        it 'invokes job' do
          worker = make_worker(TriggeredTimeout, nil, kernel_class)
          worker = worker

          worker.around_perform job

          expect(worker.around_perform_call_counter).to eq(1)
        end
      end

      context 'when timeout is not nil' do
        it 'aborts with a clear error when given a non-numeric timeout' do
          worker = make_worker(TriggeredTimeout, "123", kernel_class)

          expect {
            worker.around_perform job
          }.to raise_error(Qless::InvalidTimeoutError)
        end

        it 'aborts with a clear error when given a non-positive timeout' do
          worker = make_worker(TriggeredTimeout, -1, kernel_class)

          expect {
            worker.around_perform job
          }.to raise_error(Qless::InvalidTimeoutError)
        end

        it 'invokes job when positive timeout specified' do
          worker = make_worker(InactiveTimeout, 120, kernel_class)
          worker = worker

          worker.around_perform job

          expect(worker.around_perform_call_counter).to eq(1)
        end

        context 'when no timeout event detected' do
          it 'does not invoke timeout recovery' do
            worker = make_worker(InactiveTimeout, 120, kernel_class)

            expect(kernel_class).not_to receive(:exit!)
            expect(job).not_to receive(:reconnect_to_redis)
            expect(job).not_to receive(:fail)

            worker.around_perform job
          end
        end

        context 'when timeout event detected' do
          it 'rescues ::Timeout::Error in case of timeout' do
            worker = make_worker(TriggeredTimeout, 120, kernel_class)
            expect {
              worker.around_perform job
            }.not_to raise_error
          end

          it 'reconnects to redis (to recover in case redis causes timeout)' do
            worker = make_worker(TriggeredTimeout, 120, kernel_class)
            expect(job).to receive(:reconnect_to_redis)
            expect {
              worker.around_perform job
            }.not_to raise_error
          end

          it 'fails the job' do
            worker = make_worker(TriggeredTimeout, 120, kernel_class)
            expect(job).to receive(:fail)
              .with(anything, /Qless: job timeout \(120\) exceeded./)
            expect {
              worker.around_perform job
            }.not_to raise_error
          end

          context 'when worker does not install RequeueExceptions middleware' do
            it 'does not call neither #requeueable? nor #handle_exception' do
              worker = make_worker(TriggeredTimeout, 120, kernel_class)

              expect {
                worker.around_perform job
              }.not_to raise_error
            end
          end

          context 'when worker installs RequeueExceptions middleware' do
            it 'requeues job if JobTimedoutError requeue requested' do
              worker = make_worker(TriggeredTimeout, 120, kernel_class)
              worker.extend(RequeueExceptions)
              worker.requeue_on JobTimedoutError, delay_range: (1..2), max_attempts: 3
              allow(worker).to receive(:handle_exception).with(job, anything)

              expect {
                worker.around_perform job
              }.not_to raise_error
            end

            it 'does not requeue job if JobTimedoutError not configured' do
              worker = make_worker(TriggeredTimeout, 120, kernel_class)
              worker.extend(RequeueExceptions)
              expect(worker).not_to receive(:handle_exception).with(job, anything)

              expect {
                worker.around_perform job
              }.not_to raise_error
            end
          end

          it 'terminates the process so it is not left in an inconsistent state' \
             ' (since `Timeout` can do that)' do
            worker = make_worker(TriggeredTimeout, 120, kernel_class)
            expect(kernel_class).to receive(:exit!)
            expect {
              worker.around_perform job
            }.not_to raise_error
          end
        end
      end
    end
  end
end
