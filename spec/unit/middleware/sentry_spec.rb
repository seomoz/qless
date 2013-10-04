# Encoding: utf-8

require 'spec_helper'
require 'qless/middleware/sentry'
require 'qless'
require 'qless/worker'

module Qless
  module Middleware
    describe Sentry do
      let(:client) { instance_double('Qless::Client').as_null_object }

      let(:klass) do
        Class.new do
          def self.perform(job)
            raise 'job failure'
          end
        end
      end

      let(:time_1) { Time.utc(2012, 8, 1, 12, 30) }
      let(:time_2) { Time.utc(2012, 8, 1, 12, 31) }

      let(:history_event) do
        {
          'popped' => time_2.to_i,
          'put'    => time_1.to_i,
          'q'      => 'test_error',
          'worker' => 'Myrons-Macbook-Pro.local-44396' }
      end

      let(:job) do
        stub_const('MyJob', klass)
        Qless::Job.build(client, MyJob,
                         data: { 'some' => 'data' },
                         worker: 'w1', queue: 'q1',
                         jid: 'abc', history: [history_event],
                         tags: %w{x y}, priority: 10)

      end

      def perform_job
        worker = Qless::Workers::SerialWorker.new(double)
        worker.extend Qless::Middleware::Sentry
        worker.perform(job)
      end

      it 'logs jobs with errors to sentry' do
        sent_event = nil
        ::Raven.stub(:send) { |e| sent_event = e }

        # it's important the job still fails normally
        job.should_receive(:fail)

        perform_job

        expect(sent_event.message).to include('job failure')
        expect(sent_event.extra[:job]).to include(
          jid:       'abc',
          klass:     'MyJob',
          data:      { 'some' => 'data' },
          queue:     'q1',
          worker:    'w1',
          tags:      %w{x y},
          priority:  10
        )

        expect(sent_event.extra[:job][:history].first).to include(
          'put' => time_1.iso8601, 'popped' => time_2.iso8601
        )
      end

      it 'does not silence the original error when sentry errors' do
        ::Raven.stub(:send) { raise ::Raven::Error, 'sentry failure' }
        job.should_receive(:fail) do |_, message|
          expect(message).to include('job failure')
          expect(message).not_to include('sentry failure')
        end

        perform_job
      end
    end
  end
end
