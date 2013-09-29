# Encoding: utf-8

# The things we're testing
require 'qless'

# Spec stuff
require 'spec_helper'

module Qless
  describe Client, :integration do
    let(:queue) { client.queues['foo'] }

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
  end
end
