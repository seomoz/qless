# Encoding: utf-8

require 'json'
require 'socket'
require 'tempfile'

require 'spec_helper'

describe 'qless-stats', :integration do
  let(:path) { './exe/qless-stats' }

  def run(*args)
    `#{path} #{args.join(' ')}`
  end

  describe 'statsd subcommand' do

    def get_statsd_messages
      begin
        sock = UDPSocket.new
        sock.bind('127.0.0.1', 8125)
        yield sock
        messages = []
        while true
          begin
            messages << sock.recvfrom_nonblock(16384)[0]
          rescue IO::WaitReadable
            break
          end
        end
        return messages
      ensure
        sock.close
      end
    end

    context 'with some interesting job stats' do

      before do
        10.times do
          client.queues['queue'].put('PretendJob', {})
        end

        client.queues['queue'].pop
        client.queues['queue'].pop.fail('FailureType', 'FailureMessage')
      end

      let(:messages) do
        get_statsd_messages do
          run('statsd', '--count=1')
        end
      end

      it 'tracks paused' do
        expect(messages).to include('queues.queue.paused:0|g')
      end

      it 'tracks running jobs' do
        expect(messages).to include('queues.queue.running:1|g')
      end

      it 'tracks waiting jobs' do
        expect(messages).to include('queues.queue.waiting:8|g')
      end

      it 'tracks recurring jobs' do
        expect(messages).to include('queues.queue.recurring:0|g')
      end

      it 'tracks depends jobs' do
        expect(messages).to include('queues.queue.depends:0|g')
      end

      it 'tracks stalled jobs' do
        expect(messages).to include('queues.queue.stalled:0|g')
      end

      it 'tracks scheduled jobs' do
        expect(messages).to include('queues.queue.scheduled:0|g')
      end

      it 'tracks completed jobs' do
        expect(messages).to include('queues.queue.completed:0|g')
      end

      it 'tracks popped jobs' do
        expect(messages).to include('queues.queue.popped:2|g')
      end

      it 'tracks failed counts' do
        expect(messages).to include('queues.queue.failed:1|g')
      end

      it 'tracks failure counts' do
        expect(messages).to include('queues.queue.failures:1|g')
      end

      it 'tracks retries' do
        expect(messages).to include('queues.queue.retries:0|g')
      end

      it 'tracks average run time' do
        found = messages.select { |m| m.include?('queues.queue.run.avg') }
        expect(found).not_to be_empty
      end

      it 'tracks std deviation of run time' do
        found = messages.select { |m| m.include?('queues.queue.run.std-dev') }
        expect(found).not_to be_empty
      end

      it 'tracks average wait time' do
        found = messages.select { |m| m.include?('queues.queue.wait.avg') }
        expect(found).not_to be_empty
      end

      it 'tracks std deviation of wait time' do
        found = messages.select { |m| m.include?('queues.queue.wait.std-dev') }
        expect(found).not_to be_empty
      end

      it 'tracks failure types' do
        expect(messages).to include('failures.FailureType:1|g')
      end

      it 'tracks total failures' do
        expect(messages).to include('failures:1|g')
      end

      it 'tracks worker counts' do
        expect(messages).to include('workers:1|g')
      end

    end

  end

end
