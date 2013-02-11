require 'spec_helper'
require 'qless'
require 'qless/lua_script_cache'

module Qless
  describe LuaScriptCache do
    let(:redis_1a) { fire_double("Redis", id: "redis://foo:1234/1", script: "sha") }
    let(:redis_1b) { fire_double("Redis", id: "redis://foo:1234/1", script: "sha") }
    let(:redis_2)  { fire_double("Redis", id: "redis://foo:1234/2", script: "sha") }
    let(:cache)    { LuaScriptCache.new }

    before { File.stub(read: "script content") }

    it 'returns different lua script objects when the script name is different' do
      script_1 = cache.script_for("foo", redis_1a)
      script_2 = cache.script_for("bar", redis_1a)

      expect(script_1).to be_a(LuaScript)
      expect(script_2).to be_a(LuaScript)

      expect(script_1).not_to be(script_2)

      expect(script_1.name).to eq("foo")
      expect(script_2.name).to eq("bar")
    end

    it 'returns different lua script objects when the redis connection is to a different server' do
      script_1 = cache.script_for("foo", redis_1a)
      script_2 = cache.script_for("foo", redis_2)

      expect(script_1).to be_a(LuaScript)
      expect(script_2).to be_a(LuaScript)

      expect(script_1).not_to be(script_2)

      expect(script_1.redis).to be(redis_1a)
      expect(script_2.redis).to be(redis_2)
    end

    it 'returns the same lua script object when the script name and redis connection are the same' do
      script_1 = cache.script_for("foo", redis_1a)
      script_2 = cache.script_for("foo", redis_1a)

      expect(script_1).to be(script_2)
    end

    it 'returns the same lua script object when the script name and redis conneciton URL are the same' do
      script_1 = cache.script_for("foo", redis_1a)
      script_2 = cache.script_for("foo", redis_1b)

      expect(script_1).to be(script_2)
    end
  end
end

