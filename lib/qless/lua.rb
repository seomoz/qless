ROOT = Pathname.new File.expand_path('./../..', File.dirname(__FILE__))

module Qless
  class Lua
    def initialize(name, redis)
      @sha   = nil
      @name  = name
      @redis = redis
      @path  = File.join(ROOT, 'qless-core', @name + '.lua')
      reload()
    end
    
    def reload()
      File.open(@path, 'r') do |f|
        @sha = @redis.script(:load, f.read())
      end
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