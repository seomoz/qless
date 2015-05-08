# Encoding: utf-8

# The things we're testing
require 'qless'

# Spec stuff
require 'spec_helper'

module Qless
  describe Queue, :integration do
    let(:queue) { client.queues['foo'] }

    it 'provides access to jobs in different states' do
      queue.put('Foo', {})
      [:depends, :running, :stalled, :scheduled, :recurring].each do |cmd|
        expect(queue.jobs.send(cmd)).to eq([])
      end
    end
    
    it 'provides access to job counts' do
      queue.put('Foo', {})
      expect(queue.counts).to eq({
        'depends'   => 0,
        'name'      => 'foo',
        'paused'    => false,
        'recurring' => 0,
        'scheduled' => 0,
        'running'   => 0,
        'stalled'   => 0,
        'waiting'   => 1
      })
    end

    def remembered_queue_names
      client.queues.counts.map { |q| q["name"] }
    end

    it 'can forget an empty queue' do
      jid = queue.put('Foo', {})
      client.bulk_cancel([jid])

      expect {
        queue.forget
      }.to change { remembered_queue_names }.from([queue.name]).to([])
    end

    it 'prevents you from forgetting a queue with jobs' do
      queue.put('Foo', {})

      expect {
        expect {
          queue.forget
        }.to raise_error(Qless::Queue::QueueNotEmptyError)
      }.not_to change { remembered_queue_names }
    end

    it 'provides access to the heartbeat configuration' do
      original = queue.heartbeat
      queue.heartbeat = 10
      expect(queue.heartbeat).to_not eq(original)
    end

    it 'provides an array of jobs when using multi-pop' do
      2.times { queue.put('Foo', {}) }
      expect(queue.pop(10).length).to eq(2)
    end

    it 'exposes queue peeking' do
      queue.put('Foo', {}, jid: 'jid')
      expect(queue.peek.jid).to eq('jid')
    end

    it 'provides an array of jobs when using multi-peek' do
      2.times { queue.put('Foo', {}) }
      expect(queue.peek(10).length).to eq(2)
    end

    it 'exposes queue statistics' do
      expect(queue.stats).to be
    end

    it 'exposes the length of the queue' do
      expect {
        jid = queue.put('Foo', {}) # waiting
        queue.put('Foo', {}); queue.pop # running
        queue.put('Foo', {}, delay: 100000) # scheduled
        queue.put('Foo', {}, depends: [jid]) # depends
      }.to change(queue, :length).from(0).to(4)
    end

    it 'can pause and unpause itself' do
      expect(queue.paused?).to be(false)
      queue.pause
      expect(queue.paused?).to be(true)
      queue.unpause
      expect(queue.paused?).to be(false)
    end

    it 'can optionally stop all running jobs when pausing' do
      pending('this is specific to ruby')
    end

    it 'exposes max concurrency' do
      queue.max_concurrency = 5
      expect(queue.max_concurrency).to eq(5)
    end

    it 'gets nil for popping an empty queue' do
      expect(queue.pop).to_not be
    end

    it 'gets nil for peeking an empty queue' do
      expect(queue.peek).to_not be
    end
  end
end
