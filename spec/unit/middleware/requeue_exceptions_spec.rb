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

      let(:container) do
        container = container_class.new
        container.extend(RequeueExceptions)
        container
      end

      describe ".requeue_on" do
        it "does not throw with empty class list" do
          container.requeue_on(delay_range: 1..10,
                               max_attempts: 1)
        end

        it "throws KeyError if no max_attempts" do
          expect do
            container.requeue_on(delay_range: 1..10)
          end.to raise_error(KeyError)
        end

        it "throws KeyError if no delay_range" do
          expect do
            container.requeue_on(max_attempts: 1)
          end.to raise_error(KeyError)
        end

        it "throws NoMethodError if delay_range does not respond to .min or .max" do
          expect do
            container.requeue_on(delay_range: 1, max_attempts: 1)
          end.to raise_error(NoMethodError)
        end

        it "throws ArgumentError if delay_range is not numerical" do
          expect do
            container.requeue_on(delay_range: "a".."z", max_attempts: 1)
          end.to raise_error(ArgumentError)
        end

        it "throws TypeError if delay_range is empty" do
          expect do
            container.requeue_on(delay_range: 2..1, max_attempts: 1)
          end.to raise_error(TypeError)
        end

        it "throws TypeError on empty delay_range" do
          expect do
            container.requeue_on(delay_range: 1..0, max_attempts: 1)
          end.to raise_error(TypeError)
        end

        it "adds exceptions to requeable collection on success" do
          container.requeue_on(ArgumentError, TypeError, delay_range: 1..2, max_attempts: 2)
          expect(container.requeueable_exceptions).to include(ArgumentError, TypeError)
        end

        it "updates exceptions on repeated .requeue_on" do
          container.requeue_on(ArgumentError, TypeError, delay_range: 1..2, max_attempts: 2)
          container.requeue_on(TypeError, KeyError, delay_range: 1..2, max_attempts: 3)
          expect(container.requeueable_exceptions).to include(ArgumentError, TypeError, KeyError)
          expect(container.requeueable_exceptions[KeyError].max_attempts).to eq(3);
        end
      end

      describe ".requeueable?" do
        before do
          container.requeue_on(KeyError, delay_range: 1..2, max_attempts: 3)
        end

        it 'returns false if exception is not requeue_on' do
          expect(container.requeueable?(TypeError)).to be(false)
        end

        it 'returns true when exception requeued on' do
          expect(container.requeueable?(KeyError)).to be(true)
        end
      end

      context "when requeue_on successful" do

        let(:job) do
          instance_double(
            'Qless::Job', requeue: nil, queue_name: 'my-queue', data: {})
        end
        let(:delay_range) { (0..30) }
        let(:max_attempts) { 20 }

        matched_exception_1 = ZeroDivisionError
        matched_exception_2 = KeyError
        unmatched_exception = RegexpError

        module MessageSpecificException
          def self.===(other)
            ArgumentError === other && other.message.include?("foo")
          end
        end

        before do
          ## container.extend(RequeueExceptions)
          container.requeue_on(matched_exception_1, matched_exception_2,
                               MessageSpecificException,
                               delay_range: delay_range,
                               max_attempts: max_attempts)
        end

        def set_requeue_callback
          container.use_on_requeue_callback { |error, job| callback_catcher << [error, job] }
        end

        def callback_catcher
          @callback_catcher ||= []
        end

        def perform
          container.around_perform(job)
        end

        describe '.use_on_requeue_callback' do
          it 'uses a default callback if none is given' do
            expect(container.on_requeue_callback).to eq(
              RequeueExceptions::DEFAULT_ON_REQUEUE_CALLBACK)
          end

          it 'accepts a block to set an after requeue callback' do
            container.use_on_requeue_callback { |*| true }
            expect(container.on_requeue_callback).not_to eq(
              RequeueExceptions::DEFAULT_ON_REQUEUE_CALLBACK)
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
            before { set_requeue_callback }

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
            job.should_receive(:requeue) do |qname, hash|
              expect(qname).to eq('my-queue')
              expect(hash[:delay]).to be_between(delay_range.min, delay_range.max)
            end
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
            before { set_requeue_callback }

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
end
