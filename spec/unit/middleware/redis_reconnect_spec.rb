require 'spec_helper'
require 'qless/middleware/redis_reconnect'
require 'redis'
require 'qless/worker'

module Qless
  module Middleware
    describe RedisReconnect do
      let(:url_1) { "redis://localhost:1234/2" }
      let(:url_2) { "redis://localhost:4321/3" }

      it 'includes the redis connection strings in its description' do
        redis_1 = Redis.new(url: url_1)
        redis_2 = Redis.new(url: url_2)
        middleware = Qless::Middleware::RedisReconnect.new(redis_1, redis_2)

        expect(middleware.inspect).to include(url_1, url_2)
        expect(middleware.to_s).to include(url_1, url_2)
      end

      it 'reconnects to the given redis clients before performing the job' do
        events = []

        stub_const("MyJob", Class.new {
          define_singleton_method :perform do |job|
            events << :performed
          end
        })

        redis_connections = 1.upto(2).map do |i|
          client = fire_double("Redis::Client")
          client.stub(:reconnect) { events << :"reconnect_#{i}" }
          fire_double("Redis", client: client)
        end

        worker = Qless::Worker.new(stub)
        worker.extend Qless::Middleware::RedisReconnect.new(*redis_connections)
        worker.perform(Qless::Job.build(stub.as_null_object, MyJob))

        expect(events).to eq([:reconnect_1, :reconnect_2, :performed])
      end
    end
  end
end

