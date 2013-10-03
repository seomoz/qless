# Encoding: utf-8

require 'thread'

module Qless
  # A class used for subscribing to messages in a thread
  class Subscriber
    # An exception that can be raised when we want the subscriber to stop
    StopListeningException = Class.new(Exception)

    def self.start(*args, &block)
      new(*args, &block).tap(&:start)
    end

    attr_reader :channel, :redis

    def initialize(client, channel, &message_received_callback)
      @channel = channel
      @message_received_callback = message_received_callback

      # pub/sub blocks the connection so we must use a different redis
      # connection
      @redis = client.new_redis_connection
    end

    # Start a thread listening
    def start
      @thread = Thread.start do
        @redis.subscribe(@channel) do |on|
          on.message do |_channel, message|
            begin
              @message_received_callback.call(self, JSON.parse(message))
            rescue StopListeningException
              @redis.unsubscribe(@channel)
            rescue Exception => error
              puts "Error: #{error}"
            end
          end
        end
      end
    end

    def stop
      # We'll raise an exception in the listener thread and then join it
      # which in turn raises the exception in this thread
      @thread.raise(StopListeningException.new)
      @thread.join
    rescue StopListeningException
      # The thread's joined and we're done here.
    end
  end
end
