# Encoding: utf-8

require 'spec_helper'
require 'qless/queue'
require 'qless/job_reservers/shuffled_round_robin'

module Qless
  module JobReservers
    describe ShuffledRoundRobin do
      let(:q1) { instance_double('Qless::Queue') }
      let(:q2) { instance_double('Qless::Queue') }
      let(:q3) { instance_double('Qless::Queue') }

      let(:queue_list) { [q1, q2, q3] }

      def new_reserver
        ShuffledRoundRobin.new(queue_list)
      end

      def stub_queue_names
        q1.stub(:name) { 'Queue1' }
        q2.stub(:name) { 'Queue2' }
        q3.stub(:name) { 'Queue3' }
      end

      let(:reserver) { new_reserver }

      describe '#reserve' do
        it 'round robins the queues' do
          queue_list.stub(shuffle: queue_list)

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

      it 'shuffles the queues so that things are distributed more easily' do
        order = []

        [q1, q2, q3].each do |q|
          q.stub(:pop) { order << q }
        end

        uniq_orders = 10.times.map do
          order.clear
          reserver = new_reserver
          3.times { reserver.reserve }
          order.dup
        end.uniq

        expect(uniq_orders).to have_at_least(3).different_orders
      end

      it 'does not change the passed queue list as a side effect' do
        orig_list = queue_list.dup

        10.times do
          new_reserver
          expect(queue_list).to eq(orig_list)
        end
      end

      describe '#prep_for_work!' do
        before { stub_queue_names }

        it 'reshuffles the queues' do
          reserver = new_reserver

          uniq_orders = 10.times.map { reserver.prep_for_work! }

          expect(uniq_orders).to have_at_least(3).different_orders
        end

        it 'resets the description to match the new queue ordering' do
          reserver = new_reserver
          initial_description = reserver.description

          reserver.prep_for_work!

          expect(reserver.description).not_to be(initial_description)
        end
      end

      describe '#description' do
        before { stub_queue_names }

        it 'returns a useful human readable string' do
          queue_list.stub(shuffle: [q2, q1, q3])

          reserver.description.should eq(
            'Queue2, Queue1, Queue3 (shuffled round robin)')
        end
      end
    end
  end
end
