module Qless
  module Middleware
    module RedisReconnect
      def self.new(*redis_connections, &block)
        Module.new do
          define_singleton_method :to_s do
            "Qless::Middleware::RedisReconnect"
          end

          block ||= lambda { |job| redis_connections }

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

