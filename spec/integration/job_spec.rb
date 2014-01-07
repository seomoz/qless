# Encoding: utf-8

# The things we're testing
require 'qless'

# Spec stuff
require 'spec_helper'
require 'timecop'

module Qless
  # This class does not have a perform method
  class NoPerformJob; end

  describe Job, :integration do
    let(:queue) { client.queues['foo'] }

    it 'can specify a jid in put and klass as string' do
      queue.put('Qless::Job', {}, jid: 'a').should eq('a')
      queue.put(Job, {}, jid: 'b').should eq('b')
    end

    it 'has all the attributes we would expect' do
      queue.put('Foo', { whiz: 'bang' }, jid: 'jid', tags: ['foo'], retries: 3)
      job = client.jobs['jid']
      expected = {
        jid: 'jid',
        data: { 'whiz' => 'bang' },
        tags: ['foo'],
        priority: 0,
        expires_at: 0,
        dependents: [],
        klass_name: 'Foo',
        queue_name: 'foo',
        worker_name: '',
        retries_left: 3,
        dependencies: [],
        original_retries: 3,
      }
      expected.each do |key, value|
        expect(job.send(key)).to eq(value)
      end
    end

    it 'can set its own priority' do
      queue.put('Foo', {}, jid: 'jid', priority: 0)
      expect(client.jobs['jid'].priority).to eq(0)
      client.jobs['jid'].priority = 10
      expect(client.jobs['jid'].priority).to eq(10)
    end

    it 'exposes its queue object' do
      queue.put('Foo', {}, jid: 'jid')
      job = client.jobs['jid']
      expect(job.queue.name).to eq(job.queue_name)
      expect(job.queue).to be_kind_of(Queue)
    end

    it 'exposes its klass' do
      queue.put(Job, {}, jid: 'jid')
      expect(client.jobs['jid'].klass).to eq(Job)
    end

    it 'exposes its ttl' do
      client.config['heartbeat'] = 10
      queue.put(Job, {}, jid: 'jid')
      job = queue.pop
      expect(9...10).to include(job.ttl)
    end

    it 'exposes its cancel method' do
      queue.put('Foo', {}, jid: 'jid')
      client.jobs['jid'].cancel
      expect(client.jobs['jid']).to_not be
    end

    it 'can tag itself' do
      queue.put('Foo', {}, jid: 'jid')
      client.jobs['jid'].tag('foo')
      expect(client.jobs['jid'].tags).to eq(['foo'])
    end

    it 'can untag itself' do
      queue.put('Foo', {}, jid: 'jid', tags: ['foo'])
      client.jobs['jid'].untag('foo')
      expect(client.jobs['jid'].tags).to eq([])
    end

    it 'exposes data through []' do
      queue.put('Foo', { whiz: 'bang' }, jid: 'jid')
      expect(client.jobs['jid']['whiz']).to eq('bang')
    end

    it 'exposes data through []=' do
      queue.put('Foo', {}, jid: 'jid')
      job = client.jobs['jid']
      job['foo'] = 'bar'
      expect(job['foo']).to eq('bar')
    end

    it 'can move itself' do
      queue.put('Foo', {}, jid: 'jid')
      client.jobs['jid'].move('bar')
      expect(client.jobs['jid'].queue_name).to eq('bar')
    end

    it 'can complete itself' do
      queue.put('Foo', {}, jid: 'jid')
      queue.pop.complete
      expect(client.jobs['jid'].state).to eq('complete')
    end

    it 'can advance itself to another queue' do
      queue.put('Foo', {}, jid: 'jid')
      queue.pop.complete('bar')
      expect(client.jobs['jid'].state).to eq('waiting')
    end

    it 'can heartbeat itself' do
      client.config['heartbeat'] = 10
      queue.put('Foo', {}, jid: 'jid')
      job = queue.pop
      before = job.ttl
      client.config['heartbeat'] = 20
      job.heartbeat
      expect(job.ttl).to be > before
    end

    it 'raises an error if it fails to heartbeat' do
      queue.put('Foo', {}, jid: 'jid')
      expect { client.jobs['jid'].heartbeat }.to raise_error
    end

    it 'knows if it is tracked' do
      queue.put('Foo', {}, jid: 'jid')
      expect(client.jobs['jid'].tracked).to be(false)
      client.jobs['jid'].track
      expect(client.jobs['jid'].tracked).to be(true)
      client.jobs['jid'].untrack
      expect(client.jobs['jid'].tracked).to be(false)
    end

    it 'can add and remove dependencies' do
      queue.put('Foo', {}, jid: 'a')
      queue.put('Foo', {}, jid: 'b')
      queue.put('Foo', {}, jid: 'c', depends: ['a'])
      expect(client.jobs['c'].dependencies).to eq(['a'])
      client.jobs['c'].depend('b')
      expect(client.jobs['c'].dependencies).to eq(%w{a b})
      client.jobs['c'].undepend('a')
      expect(client.jobs['c'].dependencies).to eq(['b'])
    end

    it 'raises an error if retry fails' do
      queue.put('Foo', {}, jid: 'jid')
      expect { client.jobs['jid'].retry }.to raise_error
    end

    it 'has a reasonable to_s' do
      queue.put('Foo', {}, jid: 'jid')
      expect(client.jobs['jid'].to_s).to eq(
        '<Qless::Job Foo (jid / foo / waiting)>')
    end

    it 'fails to process if it does not have the method' do
      queue.put(NoPerformJob, {}, jid: 'jid')
      queue.pop.perform
      job = client.jobs['jid']
      expect(job.state).to eq('failed')
      expect(job.failure['group']).to eq('foo-method-missing')
    end

    it 'raises an error if it cannot find the class' do
      queue.put('Foo::Whiz::Bang', {}, jid: 'jid')
      queue.pop.perform
      job = client.jobs['jid']
      expect(job.state).to eq('failed')
      expect(job.failure['group']).to eq('foo-NameError')
    end

    it 'exposes failing a job' do
      queue.put('Foo', {}, jid: 'jid')
      queue.pop.fail('foo', 'message')
      expect(client.jobs['jid'].state).to eq('failed')
      expect(client.jobs['jid'].failure['group']).to eq('foo')
      expect(client.jobs['jid'].failure['message']).to eq('message')
    end

    it 'only invokes before_complete on an already-completed job' do
      queue.put('Foo', {})
      job = queue.pop
      job.fail('foo', 'some message')

      events = []
      job.before_complete { events << :before }
      job.after_complete  { events << :after  }

      expect do
        job.complete
      end.to raise_error(Qless::Job::CantCompleteError, /failed/)

      expect(events).to eq([:before])
    end

    it 'provides access to #log' do
      queue.put('Foo', {}, jid: 'jid')
      # Both with and without data
      client.jobs['jid'].log('hello')
      client.jobs['jid'].log('hello', { foo: 'bar'} )
      history = client.jobs['jid'].raw_queue_history
      expect(history[1]['what']).to eq('hello')
      expect(history[2]['foo']).to eq('bar')
    end
  end

  describe RecurringJob, :integration do
    let(:queue) { client.queues['foo'] }

    it 'can take either a class or string' do
      queue.recur('Qless::Job', {}, 5, jid: 'a').should eq('a')
      queue.recur(Job, {}, 5, jid: 'b').should eq('b')
    end

    it 'has all the expected attributes' do
      queue.recur('Foo', { whiz: 'bang' }, 60, jid: 'jid', tags: ['foo'],
                  retries: 3)
      job = client.jobs['jid']
      expected = {
        :jid        => 'jid',
        :data       => {'whiz' => 'bang'},
        :tags       => ['foo'],
        :count      => 0,
        :backlog    => 0,
        :retries    => 3,
        :interval   => 60,
        :priority   => 0,
        :queue_name => 'foo',
        :klass_name => 'Foo'
      }
      expected.each do |key, value|
        expect(job.send(key)).to eq(value)
      end
    end

    it 'can set its priority' do
      queue.recur('Foo', {}, 60, jid: 'jid', priority: 0)
      client.jobs['jid'].priority = 10
      expect(client.jobs['jid'].priority).to eq(10)
    end

    it 'can set its retries' do
      queue.recur('Foo', {}, 60, jid: 'jid', retries: 2)
      client.jobs['jid'].retries = 5
      expect(client.jobs['jid'].retries).to eq(5)
    end

    it 'can set its interval' do
      queue.recur('Foo', {}, 60, jid: 'jid')
      client.jobs['jid'].interval = 10
      expect(client.jobs['jid'].interval).to eq(10)
    end

    it 'can set its data' do
      queue.recur('Foo', {}, 60, jid: 'jid')
      client.jobs['jid'].data = { 'foo' => 'bar' }
      expect(client.jobs['jid'].data).to eq({ 'foo' => 'bar' })
    end

    it 'can set its klass' do
      queue.recur('Foo', {}, 60, jid: 'jid')
      client.jobs['jid'].klass = Job
      expect(client.jobs['jid'].klass).to eq(Job)
    end

    it 'can set its queue' do
      queue.recur('Foo', {}, 60, jid: 'jid')
      client.jobs['jid'].move('bar')
      expect(client.jobs['jid'].queue_name).to eq('bar')
    end

    it 'can set its backlog' do
      queue.recur('Foo', {}, 60, jid: 'jid', backlog: 10)
      client.jobs['jid'].backlog = 1
      expect(client.jobs['jid'].backlog).to eq(1)
    end

    it 'exposes when the next job will run' do
      pending('This is implemented only in the python client')
      queue.recur('Foo', {}, 60, jid: 'jid')
      nxt = client.jobs['jid'].next
      queue.pop
      expect(client.jobs['jid'].next - nxt - 60).to be < 1
    end

    it 'can cancel itself' do
      queue.recur('Foo', {}, 60, jid: 'jid')
      client.jobs['jid'].cancel
      expect(client.jobs['jid']).to_not be
    end

    it 'can set its tags' do
      queue.recur('Foo', {}, 60, jid: 'jid')
      client.jobs['jid'].tag('foo')
      expect(client.jobs['jid'].tags).to eq(['foo'])
      client.jobs['jid'].untag('foo')
      expect(client.jobs['jid'].tags).to eq([])
    end

    describe 'last spawned job access' do
      it 'exposes the jid and job of the last spawned job' do
        queue.recur('Foo', {}, 60, jid: 'jid')

        Timecop.travel(Time.now + 121) do # give it enough time to spawn 2 jobs
          last_spawned = queue.peek(2).max_by(&:initially_put_at)

          job = client.jobs['jid']
          expect(job.last_spawned_jid).to eq(last_spawned.jid)
          expect(job.last_spawned_job).to eq(last_spawned)
        end
      end

      it 'returns nil if no job has ever been spawned' do
        queue.recur('Foo', {}, 60, jid: 'jid')
        job = client.jobs['jid']

        expect(job.last_spawned_jid).to be_nil
        expect(job.last_spawned_job).to be_nil
      end
    end
  end
end
