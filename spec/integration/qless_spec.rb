# Encoding: utf-8

# The things we're testing
require 'qless'

# Spec stuff
require 'spec_helper'

module Qless
  describe Client, :integration do
    let(:queue) { client.queues['foo'] }

    describe ClientJobs do
      it 'provides access to multiget' do
        queue.put('Foo', {}, jid: 'jid')
        jobs = client.jobs.multiget('jid', 'nonexistent')
        expect(jobs.length).to eq(1)
        expect(jobs[0].jid).to eq('jid')
      end
    end

    it 'can spawn a new connection when given a redis connection' do
      redis  = new_redis_for_alternate_db
      client = ::Qless::Client.new(redis: redis)
      redis2 = client.new_redis_connection
      expect(redis2).not_to equal(client.redis)
      expect(redis2.id).to eq(client.redis.id)
    end

    describe '#workers' do
      it 'provides access to worker stats' do
        # Put the job, there should be no workers
        queue.put('Foo', {}, jid: 'jid')
        expect(client.workers.counts).to eq({})

        # Pop a job and we have some information
        queue.pop
        expect(client.workers.counts).to eq([{
          'name'    => queue.worker_name,
          'jobs'    => 1,
          'stalled' => 0
        }])
        expect(client.workers[queue.worker_name]).to eq({
          'jobs'    => ['jid'],
          'stalled' => {}
        })
      end

      it 'can deregister workers' do
        # Ensure there's a worker listed
        queue.put('Foo', {}, jid: 'jid')
        queue.pop

        # Deregister and make sure it goes away
        client.deregister_workers(queue.worker_name)
        expect(client.workers.counts).to eq({})
      end
    end

    it 'exposes tracking jobs' do
      queue.put('Foo', {}, jid: 'jid')
      client.track('jid')
      expect(client.jobs['jid'].tracked).to eq(true)
    end

    it 'exposes untrack jobs' do
      queue.put('Foo', {}, jid: 'jid')
      client.jobs['jid'].track
      client.untrack('jid')
      expect(client.jobs['jid'].tracked).to eq(false)
    end

    it 'exposes top tags' do
      10.times do
        queue.put('Foo', {}, tags: %w{foo bar whiz})
      end
      expect(client.tags.to_set).to eq(%w{foo bar whiz}.to_set)
    end

    it 'shows empty tagged jobs as an array' do
      # If there are no jobs with a given tag, it should be an array, not a hash
      expect(client.jobs.tagged('foo')['jobs']).to eq([])
    end

    it 'exposes bulk cancel' do
      jids = 10.times.map { queue.put('Foo', {}) }
      client.bulk_cancel(jids)
      jobs = jids.map { |jid| client.jobs[jid] }
      expect(jobs.compact).to eq([])
    end

    it 'exposes a sane inspect' do
      expect(client.inspect).to include('Qless::Client')
    end
  end
end
