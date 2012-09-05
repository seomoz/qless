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
    
    def call(keys, args)
      begin
        return @redis.evalsha(@sha, keys.length, *(keys + args))
      rescue
        reload
        return @redis.evalsha(@sha, keys.length, *(keys + args))
      end
    end
  end
end
