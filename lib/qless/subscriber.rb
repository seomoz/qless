require 'thread'
require 'qless/wait_until'

module Qless
  class Subscriber
    def self.start(*args, &block)
      new(*args, &block).tap(&:start_pub_sub_listener)
    end

    attr_reader :client, :channel

    def initialize(client, channel, &message_received_callback)
      @client = client
      @channel = channel
      @message_received_callback = message_received_callback

      # pub/sub blocks the connection so we must use a different redis connection
      @client_redis = client.redis
      @listener_redis = client.new_redis_connection

      @my_channel = Qless.generate_jid
    end

    def start_pub_sub_listener
      @thread = ::Thread.start do
        @listener_redis.subscribe(channel, @my_channel) do |on|
          on.message do |_channel, message|
            if _channel == @my_channel
              @listener_redis.unsubscribe(channel, @my_channel) if message == 'disconnect'
            else
              @message_received_callback.call(self, JSON.parse(message))
            end
          end
        end
      end

      wait_until_thread_listening
    end

    def stop
      @client_redis.publish(@my_channel, 'disconnect')

      Qless::WaitUntil.wait_until(2) do
        !Thread.list.include?(@thread)
      end
    end

  private

    def wait_until_thread_listening
      Qless::WaitUntil.wait_until(10) do
        @client_redis.publish(@my_channel, 'listening?') == 1
      end
    end
  end
end


