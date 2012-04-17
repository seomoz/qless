require 'qless/core'

module Qless
  class Lua
    def initialize(name, redis)
      @sha   = nil
      @name  = name
      @redis = redis
      reload()
    end
    
    def reload()
      @sha = @redis.script(:load, Qless::Core.script_contents(@name))
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
