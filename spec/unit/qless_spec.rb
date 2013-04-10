require 'spec_helper'
require 'qless'

describe Qless do
  describe ".generate_jid" do
    it "generates a UUID suitable for use as a jid" do
      Qless.generate_jid.should match(/\A[a-f0-9]{32}\z/)
    end
  end

  describe ".worker_name" do
    it 'includes the hostname in the worker name' do
      Qless.worker_name.should include(Socket.gethostname)
    end

    it 'includes the pid in the worker name' do
      Qless.worker_name.should include(Process.pid.to_s)
    end
  end

  context 'when instantiated' do
    let(:redis) { fire_double("Redis", id: "redis://foo:1/1", info: { "redis_version" => "2.6.0" }) }
    let(:redis_class) { fire_replaced_class_double("Redis") }

    before do
      redis.stub(:script) # so no scripts get loaded
      redis_class.stub(connect: redis)
    end

    it 'raises an error if the redis version is too low' do
      redis.stub(info: { "redis_version" => '2.5.3' })
      expect { Qless::Client.new }.to raise_error(Qless::UnsupportedRedisVersionError)
    end

    it 'does not raise an error if the redis version is sufficient' do
      redis.stub(info: { "redis_version" => '2.6.0' })
      Qless::Client.new # should not raise an error
    end

    it 'does not raise an error if the redis version is a prerelease' do
      redis.stub(info: { "redis_version" => '2.6.8-pre2' })
      Qless::Client.new # should not raise an error
    end

    it 'considers 2.10 sufficient even though it is lexically sorted before 2.6' do
      redis.stub(info: { "redis_version" => '2.10.0' })
      Qless::Client.new # should not raise an error
    end

    it 'allows the redis connection to be passed directly in' do
      redis_class.should_not_receive(:connect)

      client = Qless::Client.new(redis: redis)
      client.redis.should be(redis)
    end
  end
end

