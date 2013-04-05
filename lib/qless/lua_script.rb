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

    def call(keys, argv)
      _call(keys, argv)
    rescue
      reload
      _call(keys, argv)
    end

  private

    if USING_LEGACY_REDIS_VERSION
      def _call(keys, argv)
        @redis.evalsha(@sha, keys.length, *(keys + argv))
      end
    else
      def _call(keys, argv)
        @redis.evalsha(@sha, keys: keys, argv: argv)
      end
    end

    def script_contents
      @script_contents ||= File.read(File.join(LUA_SCRIPT_DIR, "#{@name}.lua"))
    end
  end
end
