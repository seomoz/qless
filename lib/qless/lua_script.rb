require 'digest/sha1'

module Qless
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
      _call(*argv)
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
