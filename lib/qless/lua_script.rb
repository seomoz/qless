module Qless
  class LuaScript
    LUA_SCRIPT_DIR = File.expand_path("../qless-core/", __FILE__)

    def initialize(name, redis, sha = nil)
      @sha   = sha
      @name  = name
      @redis = redis
      reload() unless sha
    end

    attr_reader :name, :redis, :sha

    def reload()
      @sha = @redis.script(:load, File.read(File.join(LUA_SCRIPT_DIR, "#{@name}.lua")))
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
  end
end
