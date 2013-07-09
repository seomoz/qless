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
    let(:redis)  { client.redis }
    let(:script) { "-- some comments\n return Qless.config.get(ARGV[1]) * ARGV[2]" }
    let(:plugin) { LuaPlugin.new("my_plugin", redis, script) }

    it 'supports Qless lua plugins' do
      client.config["heartbeat"] = 14
      expect(plugin.call "heartbeat", 3).to eq(14 * 3)
    end

    RSpec::Matchers.define :string_excluding do |snippet|
      match do |actual|
        !actual.include?(snippet)
      end
    end

    it 'strips out comment lines before sending the script to redis' do
      redis.should_receive(:script)
           .with(:load, string_excluding("some comments"))
           .at_least(:once)
           .and_call_original

      client.config["heartbeat"] = 16
      expect(plugin.call "heartbeat", 3).to eq(16 * 3)
    end

    it 'does not load the script extra times' do
      redis.should_receive(:script)
           .with(:load, an_instance_of(String))
           .once
           .and_call_original

      3.times do
        plugin = LuaPlugin.new("my_plugin", redis, script)
        plugin.call("heartbeat", 3)
      end
    end
  end
end

