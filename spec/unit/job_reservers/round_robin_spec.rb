require 'spec_helper'
require 'qless/queue'
require 'qless/job_reservers/round_robin'

module Qless
  module JobReservers
    describe RoundRobin do
      let(:q1) { fire_double("Qless::Queue") }
      let(:q2) { fire_double("Qless::Queue") }
      let(:q3) { fire_double("Qless::Queue") }
      let(:reserver) { RoundRobin.new([q1, q2, q3]) }

      describe "#reserve" do
        it 'round robins the queues' do
          q1.should_receive(:pop).twice { :q1_job }
          q2.should_receive(:pop).once  { :q2_job }
          q3.should_receive(:pop).once  { :q3_job }

          reserver.reserve.should eq(:q1_job)
          reserver.reserve.should eq(:q2_job)
          reserver.reserve.should eq(:q3_job)
          reserver.reserve.should eq(:q1_job)
        end

        it 'returns nil if none of the queues have jobs' do
          q1.should_receive(:pop).once { nil }
          q2.should_receive(:pop).once { nil }
          q3.should_receive(:pop).once { nil }
          reserver.reserve.should be_nil
        end
      end

      describe "#description" do
        it 'returns a useful human readable string' do
          q1.stub(:name) { "Queue1" }
          q2.stub(:name) { "Queue2" }
          q3.stub(:name) { "Queue3" }

          reserver.description.should eq("Queue1, Queue2, Queue3 (round robin)")
        end
      end
    end
  end
end

