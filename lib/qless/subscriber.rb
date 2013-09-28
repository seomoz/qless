# Encoding: utf-8

require 'thread'

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
      # We'll raise an exception in the listener thread and then join it
      # which in turn raises the exception in this thread
      @thread.raise('stop')
      @thread.join
    rescue RuntimeError
      # The thread has been joined and is now dead.
    end
  end
end
