# Encoding: utf-8

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
      let(:job) do
        instance_double(
          'Qless::Job', requeue: nil, queue_name: 'my-queue', data: {})
      end
      let(:delay_range) { (0..30) }
      let(:max_attempts) { 20 }

      matched_exception_1 = ZeroDivisionError
      matched_exception_2 = KeyError
      unmatched_exception = RegexpError

      let(:requeue_on_args) do
        [matched_exception_1, matched_exception_2, MessageSpecificException,
          delay_range: delay_range, max_attempts: max_attempts]
      end

      module MessageSpecificException
        def self.===(other)
          ArgumentError === other && other.message.include?("foo")
        end
      end

      before do
        container.extend(RequeueExceptions)
        container.requeue_on(*requeue_on_args)
      end

      def add_requeue_callback
        callback = ->(error, job) { callback_catcher << [error, job] }
        container.after_requeue_callbacks << callback
      end

      def callback_catcher
        @callback_catcher ||= []
      end

      def perform
        container.around_perform(job)
      end

      describe '.requeue_on' do
        it 'accepts a block to set an after requeue callback' do
          container.extend(RequeueExceptions)

          expect {
            container.requeue_on(*requeue_on_args) { |*| true }
          }.to change {container.after_requeue_callbacks.size }.from(0).to(1)
        end
      end

      context 'when no exception is raised' do
        before { container.perform = -> { } }

        it 'does not requeue the job' do
          job.should_not_receive(:requeue)
          perform
        end
      end

      context 'when an unmatched exception is raised' do
        before { container.perform = -> { raise unmatched_exception } }

        it 'allows the error to propagate' do
          job.should_not_receive(:requeue)
          expect { perform }.to raise_error(unmatched_exception)
        end

        context 'when an after requeue callback is set' do
          before { add_requeue_callback }

          it 'does not call the callback' do
            expect { perform }.to raise_error(unmatched_exception)

            expect(callback_catcher.size).to eq(0)
          end
        end
      end

      shared_context "requeues on matching exception" do |exception, exception_name|
        before { container.perform = -> { raise_exception } }

        it 'requeues the job' do
          job.should_receive(:requeue).with('my-queue', anything)
          perform
        end

        it 'uses a random delay from the delay_range' do
          Kernel.srand(100)
          sample = delay_range.to_a.sample

          job.should_receive(:requeue).with(
            'my-queue', hash_including(delay: sample))

          Kernel.srand(100)
          perform
        end

        it 'tracks the number of requeues for this error' do
          expected_first_time = {
            'requeues_by_exception' => { exception_name => 1 } }
          job.should_receive(:requeue).with('my-queue', hash_including(
            data: expected_first_time
          ))
          perform

          job.data.merge!(expected_first_time)

          job.should_receive(:requeue).with('my-queue', hash_including(
            data: { 'requeues_by_exception' => { exception_name => 2 } }
          ))
          perform
        end

        it 'preserves other requeues_by_exception values' do
          job.data['requeues_by_exception'] = { 'SomeKlass' => 3 }

          job.should_receive(:requeue).with('my-queue', hash_including(
            data: {
              'requeues_by_exception' => {
                exception_name => 1, 'SomeKlass' => 3
              } }
          ))
          perform
        end

        it 'preserves other data' do
          job.data['foo'] = 3

          job.should_receive(:requeue).with('my-queue', hash_including(
            data: {
              'requeues_by_exception' => { exception_name => 1 },
              'foo' => 3 }
          ))
          perform
        end

        it 'allow the error to propogate after max_attempts' do
          job.data['requeues_by_exception'] = {
            exception_name => max_attempts }
          job.should_not_receive(:requeue)

          expect { perform }.to raise_error(exception)
        end

        context 'when an after requeue callback is set' do
          before { add_requeue_callback }

          it 'calls the callback' do
            expect {
              perform
            }.to change { callback_catcher.size }.from(0).to(1)
          end
        end
      end

      context "when a matched exception is raised" do
        include_examples "requeues on matching exception", matched_exception_1, matched_exception_1.name do
          define_method(:raise_exception) { raise matched_exception_1 }
        end
      end

      context "when another matched exception is raised" do
        include_examples "requeues on matching exception", matched_exception_2, matched_exception_2.name do
          define_method(:raise_exception) { raise matched_exception_2 }
        end
      end

      context "when a subclass of a matched exception is raised" do
        exception = Class.new(matched_exception_1)
        include_examples "requeues on matching exception", exception, matched_exception_1.name do
          define_method(:raise_exception) { raise exception }
        end
      end

      context "when an exception is raised that matches a listed on using `===` but not `is_a?" do
        let(:exception_instance) { ArgumentError.new("Bad foo") }

        before do
          expect(exception_instance).not_to be_a(MessageSpecificException)
          expect(MessageSpecificException).to be === exception_instance
        end

        include_examples "requeues on matching exception", MessageSpecificException, MessageSpecificException.name do
          define_method(:raise_exception) { raise exception_instance }
        end
      end
    end
  end
end
