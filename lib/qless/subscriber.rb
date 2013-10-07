# Encoding: utf-8

require 'qless/wait_until'
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
      @log_to = options.fetch(:log_to) { $stderr }

      # pub/sub blocks the connection so we must use a different redis
      # connection
      @client_redis   = client.redis
      @listener_redis = client.new_redis_connection

      @my_channel = Qless.generate_jid
    end

    # Start a thread listening
    def start
      @thread = Thread.start do
        @listener_redis.subscribe(@channel, @my_channel) do |on|
          on.message do |_channel, message|
            handle_message(_channel, message)
          end
        end
      end

      wait_until_thread_listening
    end

    def stop
      @client_redis.publish(@my_channel, 'disconnect') == 1
      @thread.join
    end

  private

    def wait_until_thread_listening
      Qless::WaitUntil.wait_until(2) do
        @client_redis.publish(@my_channel, 'listening?') == 1
      end
    end

    def handle_message(channel, message)
      if channel == @my_channel
        @listener_redis.unsubscribe(@channel, @my_channel) if message == "disconnect"
      else
        @message_received_callback.call(self, JSON.parse(message))
      end
    rescue Exception => error
      @log_to.puts "Error: #{error}"
    end
  end
end
