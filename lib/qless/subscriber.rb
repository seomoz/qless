# Encoding: utf-8

require 'thread'

module Qless
  # A class used for subscribing to messages in a thread
  class Subscriber
    def self.start(*args, &block)
      new(*args, &block).tap(&:start)
    end

    attr_reader :channel, :redis

    def initialize(client, channel, options = {}, &message_received_callback)
      @channel = channel
      @message_received_callback = message_received_callback
      @log = options.fetch(:log) { ::Logger.new($stderr) }

      # pub/sub blocks the connection so we must use a different redis
      # connection
      @client_redis   = client.redis
      @listener_redis = client.new_redis_connection

      @my_channel = Qless.generate_jid
    end

    # Start a thread listening
    def start
      queue = ::Queue.new

      @thread = Thread.start do
        @listener_redis.subscribe(@channel, @my_channel) do |on|
          on.subscribe do |channel|
            queue.push(:subscribed) if channel == @channel
          end

          on.message do |channel, message|
            handle_message(channel, message)
          end
        end
      end

      queue.pop
    end

    def stop
      @client_redis.publish(@my_channel, 'disconnect')
      @thread.join
    end

  private

    def handle_message(channel, message)
      if channel == @my_channel
        @listener_redis.unsubscribe(@channel, @my_channel) if message == "disconnect"
      else
        @message_received_callback.call(self, JSON.parse(message))
      end
    rescue Exception => error
      @log.error("Qless::Subscriber") { error }
    end
  end
end
