# Encoding: utf-8

require 'thread'
require 'qless/wait_until'

module Qless
  # A class used for subscribing to messages in a thread
  class Subscriber
    def self.start(*args, &block)
      new(*args, &block).tap(&:start_pub_sub_listener)
    end

    attr_reader :client

    def initialize(client, channel, &message_received_callback)
      @client = client
      @channel = channel
      @message_received_callback = message_received_callback

      # pub/sub blocks the connection so we must use a different redis
      # connection
      @client_redis = client.redis
      @listener_redis = client.new_redis_connection
    end

    def start_pub_sub_listener
      @thread = ::Thread.start do
        begin
          @listener_redis.subscribe(channel) do |on|
            on.message do |_channel, message|
              @message_received_callback.call(self, JSON.parse(message))
            end
          end
        rescue
        end
      end
    end

    def unsubscribe(*args)
      @listener_redis.unsubscribe(*args)
    end

    def stop
      @thread.raise('stop')
      Qless::WaitUntil.wait_until(2) do
        !Thread.list.include?(@thread)
      end
    end
  end
end
