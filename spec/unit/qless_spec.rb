# Encoding: utf-8

require 'spec_helper'
require 'qless'

describe Qless do
  describe '.generate_jid' do
    it 'generates a UUID suitable for use as a jid' do
      Qless.generate_jid.should match(/\A[a-f0-9]{32}\z/)
    end
  end

  def redis_double(overrides = {})
    attributes = { id: 'redis://foo:1/1', info: { 'redis_version' => '2.6.0' } }
    instance_double('Redis', attributes.merge(overrides))
  end

  let(:redis) { redis_double }
  let(:redis_class) do
    class_double('Redis').as_stubbed_const(transfer_nested_constants: true)
  end

  before do
    redis.stub(:script) # so no scripts get loaded
    redis_class.stub(connect: redis)
  end

  describe '#worker_name' do
    it 'includes the hostname in the worker name' do
      Qless::Client.new.worker_name.should include(Socket.gethostname)
    end

    it 'includes the pid in the worker name' do
      Qless::Client.new.worker_name.should include(Process.pid.to_s)
    end
  end

  context 'when instantiated' do
    it 'raises an error if the redis version is too low' do
      redis.stub(info: { 'redis_version' => '2.5.3' })
      expect { Qless::Client.new }.to raise_error(
        Qless::UnsupportedRedisVersionError)
    end

    it 'does not raise an error if the redis version is sufficient' do
      redis.stub(info: { 'redis_version' => '2.6.0' })
      Qless::Client.new # should not raise an error
    end

    it 'does not raise an error if the redis version is a prerelease' do
      redis.stub(info: { 'redis_version' => '2.6.8-pre2' })
      Qless::Client.new # should not raise an error
    end

    it 'considers 2.10 sufficient 2.6' do
      redis.stub(info: { 'redis_version' => '2.10.0' })
      Qless::Client.new # should not raise an error
    end

    it 'allows the redis connection to be passed directly in' do
      redis_class.should_not_receive(:connect)

      client = Qless::Client.new(redis: redis)
      client.redis.should be(redis)
    end

    it 'creates a new redis connection based on initial redis connection options' do
      options = { host: 'localhost', port: '6379', password: 'awes0me!' }
      # Create the initial client which also instantiates an initial redis connection
      client = Qless::Client.new(options)

      # Prepare stub to ensure second connection is instantiated with the same options as initial connection
      redis_class.stub(:new).with(options)
      client.new_redis_connection
    end
  end

  describe "equality semantics" do
    it 'is considered equal to another instance connected to the same redis DB' do
      client1 = Qless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))
      client2 = Qless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))

      expect(client1 == client2).to eq(true)
      expect(client2 == client1).to eq(true)
      expect(client1.eql? client2).to eq(true)
      expect(client2.eql? client1).to eq(true)

      expect(client1.hash).to eq(client2.hash)
    end

    it 'is not considered equal to another instance connected to a different redis DB' do
      client1 = Qless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))
      client2 = Qless::Client.new(redis: redis_double(id: "redis://foo.com:1/2"))

      expect(client1 == client2).to eq(false)
      expect(client2 == client1).to eq(false)
      expect(client1.eql? client2).to eq(false)
      expect(client2.eql? client1).to eq(false)

      expect(client1.hash).not_to eq(client2.hash)
    end

    it 'is not considered equal to other types of objects' do
      client1 = Qless::Client.new(redis: redis_double(id: "redis://foo.com:1/1"))
      client2 = Class.new(Qless::Client).new(redis: redis_double(id: "redis://foo.com:1/1"))

      expect(client1 == client2).to eq(false)
      expect(client1.eql? client2).to eq(false)
      expect(client1.hash).not_to eq(client2.hash)
    end
  end
end
