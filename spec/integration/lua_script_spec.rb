require 'spec_helper'
require "qless"
require 'yaml'

module Qless
  describe LuaScript, :integration do
    let(:redis) { client.redis }

    it 'does not make any redis requests upon initialization' do
      redis = double("Redis")

      expect {
        LuaScript.new("config", redis)
      }.not_to raise_error # e.g. MockExpectationError
    end

    it 'can issue the command without loading the script if it is already loaded' do
      script = LuaScript.new("config", redis)
      redis.script(:load, script.send(:script_contents)) # to ensure its loaded
      redis.should_not_receive(:script)

      expect {
        script.call([], ['set', 'key', 3])
      }.to change { redis.keys.size }.by(1)
    end

    it 'loads the script as needed if the command fails' do
      script = LuaScript.new("config", redis)
      redis.script(:flush)

      redis.should_receive(:script).and_call_original

      expect {
        script.call([], ['set', 'key', 3])
      }.to change { redis.keys.size }.by(1)
    end
  end
end

