# Encoding: utf-8

require 'spec_helper'
require 'qless/middleware/redis_reconnect'
require 'redis'
require 'qless/worker'

module Qless
  module Middleware
    describe RedisReconnect do
      let(:url_1) { 'redis://localhost:1234/2' }
      let(:url_2) { 'redis://localhost:4321/3' }

      it 'has a human readable description' do
        redis_1 = Redis.new(url: url_1)
        redis_2 = Redis.new(url: url_2)
        middleware = Qless::Middleware::RedisReconnect.new(redis_1, redis_2)

        expect(
          middleware.inspect).to include('Qless::Middleware::RedisReconnect')
        expect(
          middleware.to_s).to include('Qless::Middleware::RedisReconnect')
      end

      def define_job_class(events)
        stub_const('MyJob', Class.new do
          define_singleton_method :perform do |job|
            events << :performed
          end
        end)
      end

      def create_redis_connections(number, events)
        number.times.map do |i|
          client = instance_double('Redis::Client')
          client.stub(:reconnect) { events << :"reconnect_#{i}" }
          instance_double('Redis', client: client)
        end
      end

      it 'reconnects to the given redis clients before performing the job' do
        define_job_class(events = [])

        redis_connections = create_redis_connections(2, events)

        worker = Qless::Workers::BaseWorker.new(double)
        worker.extend Qless::Middleware::RedisReconnect.new(*redis_connections)
        worker.perform(Qless::Job.build(double.as_null_object, MyJob))

        expect(events).to eq([:reconnect_0, :reconnect_1, :performed])
      end

      it 'allows the redis connections to be picked based on job data' do
        define_job_class(events = [])
        worker = Qless::Workers::BaseWorker.new(double)
        redis_connections = create_redis_connections(4, events)

        worker.extend Qless::Middleware::RedisReconnect.new { |job|
          if job.data['type'] == 'evens'
            [redis_connections[0], redis_connections[2]]
          else
            [redis_connections[1], redis_connections[3]]
          end
        }

        even_job = Qless::Job.build(
          double.as_null_object, MyJob, data: { 'type' => 'evens' })
        odd_job  = Qless::Job.build(
          double.as_null_object, MyJob, data: { 'type' => 'odds'  })

        worker.perform(even_job)
        expect(events).to eq([:reconnect_0, :reconnect_2, :performed])

        worker.perform(odd_job)
        expect(events).to eq([:reconnect_0, :reconnect_2, :performed,
                              :reconnect_1, :reconnect_3, :performed])
      end

      it 'allows the block to return a single redis connection' do
        define_job_class(events = [])
        worker = Qless::Workers::BaseWorker.new(double)
        redis_connections = create_redis_connections(1, events)

        worker.extend Qless::Middleware::RedisReconnect.new { |job|
          redis_connections.first
        }
        worker.perform(Qless::Job.build(double.as_null_object, MyJob))

        expect(events).to eq([:reconnect_0, :performed])
      end
    end
  end
end
