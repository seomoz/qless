module Qless
  class Lua
    LUA_SCRIPT_DIR = File.expand_path("../qless-core/", __FILE__)

    def initialize(name, redis)
      @sha   = nil
      @name  = name
      @redis = redis
      reload()
    end

    def reload()
      @sha = @redis.script(:load, File.read(File.join(LUA_SCRIPT_DIR, "#{@name}.lua")))
    end

    def call(keys, argv)
      begin
        return @redis.evalsha(@sha, keys.length, *(keys + argv)) if USING_LEGACY_REDIS_VERSION
        return @redis.evalsha(@sha, keys: keys, argv: argv)
      rescue
        reload
        return @redis.evalsha(@sha, keys.length, *(keys + argv)) if USING_LEGACY_REDIS_VERSION
        return @redis.evalsha(@sha, keys: keys, argv: argv)
      end
    end
  end
end
