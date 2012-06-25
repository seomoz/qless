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
      # true if using the 2.x version of redis gem
      use_legacy_evalsha = Gem.loaded_specs['redis'].version < Gem::Version.create('3.0')
      begin
        return @redis.evalsha(@sha, keys.length, *(keys + argv)) if use_legacy_evalsha
        return @redis.evalsha(@sha, keys: keys, argv: argv)
      rescue
        reload
        return @redis.evalsha(@sha, keys.length, *(keys + argv)) if use_legacy_evalsha
        return @redis.evalsha(@sha, keys: keys, argv: argv)
      end
    end
  end
end
