module Qless
  module Middleware
    module RedisReconnect
      def self.new(*redis_connections)
        Module.new do
          define_singleton_method :to_s do
            "Qless::Middleware::RedisReconnect(#{redis_connections.map(&:id).join(', ')})"
          end

          define_method :around_perform do |job|
            redis_connections.each do |redis|
              redis.client.reconnect
            end

            super(job)
          end
        end
      end
    end
  end
end

