# Encoding: utf-8

# The things we're testing
require 'qless'
require 'qless/subscriber'

# Spec stuff
require 'spec_helper'

module Qless
  describe Subscriber, :integration, :uses_threads do
    let(:channel) { 'foo' }

    def publish(message)
      # Keep trying to publish until someone listens
      sleep 0.01 until redis.publish(channel, message) == 1
    end

    def listen
      # Start a subscriber on our test channel
      Subscriber.start(client, channel) do |this, message|
        yield this, message
      end
    end

    it 'can listen for messages' do
      # Push messages onto the 'foo' key as they happen
      listen do |_, message|
        new_redis.rpush(channel, message)
      end

      # Wait until the message is sent
      publish('{}')
      expect(redis.brpop(channel)).to eq([channel, '{}'])
    end

    it 'does not stop listening for callback exceptions' do
      # If the callback throws an exception, it should keep listening for more
      # messages, and not fall over instead
      listen do |_, message|
        raise 'Explodify' if message['explode']
        new_redis.rpush(channel, message)
      end

      # Wait until the message is sent
      publish('{"explode": true}')
      publish('{}')
      expect(redis.brpop(channel)).to eq([channel, '{}'])
    end

    it 'can be stopped' do
      # We can start a listener and then stop a listener
      subscriber = listen do |_, message|
        new_redis.rpush(channel, message)
      end
      publish('{}')

      # Stop the subscriber and then ensure it's stopped listening
      subscriber.stop
      sleep 0.01 until redis.publish(channel, 'foo') == 0
      expect(redis.publish(channel, 'foo')).to eq(0)
    end
  end
end
