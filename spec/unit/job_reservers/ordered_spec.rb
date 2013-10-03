# Encoding: utf-8

require 'spec_helper'
require 'qless/queue'
require 'qless/job_reservers/ordered'

module Qless
  module JobReservers
    describe Ordered do
      let(:q1) { instance_double('Qless::Queue') }
      let(:q2) { instance_double('Qless::Queue') }
      let(:q3) { instance_double('Qless::Queue') }
      let(:reserver) { Ordered.new([q1, q2, q3]) }

      describe '#reserve' do
        it 'always pops jobs from the first queue as long as it has jobs' do
          q1.should_receive(:pop).and_return(:j1, :j2, :j3)
          q2.should_not_receive(:pop)
          q3.should_not_receive(:pop)

          reserver.reserve.should eq(:j1)
          reserver.reserve.should eq(:j2)
          reserver.reserve.should eq(:j3)
        end

        it 'falls back to other queues when earlier queues lack jobs' do
          call_count = 1
          q1.should_receive(:pop).exactly(4).times do
            :q1_job if [2, 4].include?(call_count)
          end
          q2.should_receive(:pop).exactly(2).times do
            :q2_job if call_count == 1
          end
          q3.should_receive(:pop).once do
            :q3_job if call_count == 3
          end

          reserver.reserve.should eq(:q2_job)
          call_count = 2
          reserver.reserve.should eq(:q1_job)
          call_count = 3
          reserver.reserve.should eq(:q3_job)
          call_count = 4
          reserver.reserve.should eq(:q1_job)
        end

        it 'returns nil if none of the queues have jobs' do
          [q1, q2, q3].each { |q| q.stub(:pop) }
          reserver.reserve.should be_nil
        end
      end

      describe '#description' do
        it 'returns a useful human readable string' do
          q1.stub(:name) { 'Queue1' }
          q2.stub(:name) { 'Queue2' }
          q3.stub(:name) { 'Queue3' }

          reserver.description.should eq('Queue1, Queue2, Queue3 (ordered)')
        end
      end
    end
  end
end
