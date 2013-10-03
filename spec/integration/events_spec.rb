# Encoding: utf-8

# The things we're testing
require 'qless'

# Spec stuff
require 'spec_helper'

module Qless
  describe ClientEvents, :integration, :uses_threads do
    let(:queue) { client.queues['foo'] }
    let(:events) { Hash.new { |h, k| h[k] = [] } }
    let(:pubsub) { new_client }

    # Tracked and untracked jobs
    let!(:untracked) do
      client.jobs[queue.put(Qless::Job, { foo: 'bar' })]
    end
    let!(:tracked) do
      client.jobs[queue.put(Qless::Job, { foo: 'bar' })].tap do |job|
        job.track
      end
    end

    # This is a thread that listens for events and makes note of them
    let!(:thread) do
      Thread.new do
        pubsub.events.listen do |on|
          # Listen for each event, and put the message into the queue
          [:canceled, :completed, :failed,
            :popped, :put, :stalled, :track, :untrack].each do |event|
            on.send(event) do |jid|
              events[event.to_s] << jid
            end
          end
        end
      end.tap { |t| t.join(0.01) }
    end

    # Wait until all the threads have sent their events
    def jids_for_event(event)
      thread.join(0.1)
      events[event]
    end

    it 'can pick up on canceled events' do
      tracked.cancel
      untracked.cancel
      expect(jids_for_event('canceled')).to eq([tracked.jid])
    end

    it 'can pick up on completion events' do
      queue.pop(10).each { |job| job.complete }
      expect(jids_for_event('completed')).to eq([tracked.jid])
    end

    it 'can pick up on failed events' do
      queue.pop(10).each { |job| job.fail('foo', 'bar') }
      expect(jids_for_event('failed')).to eq([tracked.jid])
    end

    it 'can pick up on pop events' do
      queue.pop(10)
      expect(jids_for_event('popped')).to eq([tracked.jid])
    end

    it 'can pick up on put events' do
      tracked.move('other')
      untracked.move('other')
      expect(jids_for_event('put')).to eq([tracked.jid])
    end

    it 'can pick up on stalled events' do
      client.config['grace-period'] = 0
      client.config['heartbeat'] = -60
      queue.pop(2)
      queue.pop(2)
      expect(jids_for_event('stalled')).to eq([tracked.jid])
    end
  end
end
