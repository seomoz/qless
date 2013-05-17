require 'digest/sha1'

module Qless
  LuaScriptError = Class.new(Qless::Error)

  class LuaScript
    LUA_SCRIPT_DIR = File.expand_path("../qless-core/", __FILE__)

    def initialize(name, redis)
      @name  = name
      @redis = redis
      @sha   = Digest::SHA1.hexdigest(script_contents)
    end

    attr_reader :name, :redis, :sha

    def reload()
      @sha = @redis.script(:load, script_contents)
    end

    def call(*argv)
      _call(*argv)
    rescue
      reload
      begin
        _call(*argv)
      rescue Redis::CommandError => err
        match = err.message.match('user_script:\d+:(\w+\(\).+$)')
        if match then
          raise LuaScriptError(match[1])
        end
      end
    end

  private

    if USING_LEGACY_REDIS_VERSION
      def _call(*argv)
        @redis.evalsha(@sha, 0, *argv)
      end
    else
      def _call(*argv)
        @redis.evalsha(@sha, keys: [], argv: argv)
      end
    end

    def script_contents
      @script_contents ||= File.read(File.join(LUA_SCRIPT_DIR, "#{@name}.lua"))
    end
  end
end
