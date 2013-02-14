require 'spec_helper'
require 'qless'
require 'qless/lua_script_cache'

module Qless
  describe LuaScriptCache do
    def redis_double(db_num)
      fire_double("Redis", id: "redis://foo:1234/#{db_num}").tap do |redis|
        redis.stub(:script) do |script_contents|
          script_contents
        end
      end
    end

    let(:redis_1a) { redis_double(1) }
    let(:redis_1b) { redis_double(1) }
    let(:redis_2)  { redis_double(2) }
    let(:cache)    { LuaScriptCache.new }

    before do
      File.stub(:read) do |file_name|
        file_name.split('/').last
      end
    end

    it 'loads each different script' do
      redis_1a.should_receive(:script).twice

      script_1 = cache.script_for("foo", redis_1a)
      script_2 = cache.script_for("bar", redis_1a)

      expect(script_1).to be_a(LuaScript)
      expect(script_2).to be_a(LuaScript)

      expect(script_1.name).to eq("foo")
      expect(script_2.name).to eq("bar")
    end

    it 'loads the same script each time it is needed in a different redis server' do
      redis_1a.should_receive(:script).once
      redis_2.should_receive(:script).once

      cache.script_for("foo", redis_1a)
      cache.script_for("foo", redis_2)
    end

    it 'loads a script only once when it is needed by multiple connections to the same redis server' do
      redis_1a.should_receive(:script).once
      redis_1b.should_not_receive(:script)

      cache.script_for("foo", redis_1a)
      cache.script_for("foo", redis_1b)
    end
  end
end

