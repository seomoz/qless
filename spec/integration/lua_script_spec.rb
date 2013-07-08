require 'spec_helper'
require "qless"
require 'yaml'

module Qless
  describe LuaScript, :integration do
    let(:redis) { client.redis }

    it 'does not make any redis requests upon initialization' do
      redis = double("Redis")

      expect {
        LuaScript.new("qless", redis)
      }.not_to raise_error # e.g. MockExpectationError
    end

    it 'can issue the command without loading the script if it is already loaded' do
      script = LuaScript.new("qless", redis)
      redis.script(:load, script.send(:script_contents)) # to ensure its loaded
      redis.should_not_receive(:script)

      expect {
        script.call([], ['config.set', 12345, 'key', 3])
      }.to change { redis.keys.size }.by(1)
    end

    it 'loads the script as needed if the command fails' do
      script = LuaScript.new("qless", redis)
      redis.script(:flush)

      redis.should_receive(:script).and_call_original

      expect {
        script.call([], ['config.set', 12345, 'key', 3])
      }.to change { redis.keys.size }.by(1)
    end
  end

  describe LuaPlugin, :integration do
    let(:redis) { client.redis }
    let(:script) { "return Qless.config.get(ARGV[1]) * ARGV[2]" }

    it 'supports Qless lua plugins' do
      plugin = LuaPlugin.new("my_plugin", redis, script)
      client.config["heartbeat"] = 14
      expect(plugin.call "heartbeat", 3).to eq(14 * 3)
    end
  end
end

