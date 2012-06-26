module Qless
  class Lua
    LUA_SCRIPT_DIR = File.expand_path("../qless-core/", __FILE__)

    # the #evalsha method signature changed between v2.x and v3.x of the redis ruby gem
    # to maintain backwards compatibility with v2.x of that gem we need this constant
    USE_LEGACY_EVALSHA = Gem.loaded_specs['redis'].version < Gem::Version.create('3.0.0')

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
        return @redis.evalsha(@sha, keys.length, *(keys + argv)) if USE_LEGACY_EVALSHA
        return @redis.evalsha(@sha, keys: keys, argv: argv)
      rescue
        reload
        return @redis.evalsha(@sha, keys.length, *(keys + argv)) if USE_LEGACY_EVALSHA
        return @redis.evalsha(@sha, keys: keys, argv: argv)
      end
    end
  end
end
