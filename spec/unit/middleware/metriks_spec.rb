# Encoding: utf-8

require 'spec_helper'
require 'qless/middleware/metriks'
require 'qless/worker'

module Qless
  module Middleware
    module Metriks
      shared_context 'isolate metriks' do
        before { ::Metriks::Registry.default.clear }
      end

      describe TimeJobsByClass do
        include_context 'isolate metriks'

        it 'tracks the time taken by the job, grouped by the class name' do
          stub_const('JobABC', Class.new)
          job = Qless::Job.build(double, JobABC, {})

          base_class = Class.new do
            def around_perform(job)
              sleep 0.05
            end
          end

          worker = Class.new(base_class) do
            include TimeJobsByClass
          end

          worker.new.around_perform(job)

          timer = ::Metriks.timer('qless.job-times.JobABC')
          expect(timer.max).to be_within(10).percent_of(0.05)
        end
      end

      describe CountEvents do
        include_context 'isolate metriks'

        before do
          stub_const('Class1', Class.new)
          stub_const('Class2', Class.new)
          stub_const('Class3', Class.new)
        end

        def worker(result = :complete)
          base_class = Class.new do
            define_method :around_perform do |job|
              job.instance_variable_set(:@state, result.to_s)
            end
          end

          Class.new(base_class) do
            include CountEvents.new(
              Class1 => 'foo',
              Class2 => 'bar'
            )
          end
        end

        def create_job_and_perform(klass, job_result = :complete)
          job = Qless::Job.build(double, klass, {})
          worker(job_result).new.around_perform(job)
        end

        it 'increments an event counter when a particular job completes' do
          create_job_and_perform(Class1)

          expect(::Metriks.counter('qless.job-events.foo').count).to eq(1)
          expect(::Metriks.counter('qless.job-events.bar').count).to eq(0)

          create_job_and_perform(Class2)

          expect(::Metriks.counter('qless.job-events.foo').count).to eq(1)
          expect(::Metriks.counter('qless.job-events.bar').count).to eq(1)
        end

        it 'does not increment the counter if the job fails' do
          create_job_and_perform(Class1, :failed)
          expect(::Metriks.counter('qless.job-events.foo').count).to eq(0)
        end

        it 'does not increment a counter if it is not in the given map' do
          create_job_and_perform(Class3)
          expect(::Metriks::Registry.default.each.to_a).to eq([])
        end
      end
    end
  end
end
