# Encoding: utf-8

module Qless
  module Middleware
    # A module for reconnecting to redis for each job
    module RedisReconnect
      def self.new(*redis_connections, &block)
        Module.new do
          define_singleton_method :to_s do
            'Qless::Middleware::RedisReconnect'
          end
          define_singleton_method(:inspect, method(:to_s))

          block ||= ->(job) { redis_connections }

          define_method :around_perform do |job|
            Array(block.call(job)).each do |redis|
              redis.client.reconnect
            end

            super(job)
          end
        end
      end
    end
  end
end
