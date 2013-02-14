require 'qless/lua_script'

module Qless
  class LuaScriptCache
    def initialize
      @sha_cache = {}
    end

    def script_for(script_name, redis_connection)
      key = CacheKey.new(script_name, redis_connection.id)

      sha = @sha_cache.fetch(key) do
        @sha_cache[key] = LuaScript.new(script_name, redis_connection).sha
      end

      LuaScript.new(script_name, redis_connection, sha)
    end

    CacheKey = Struct.new(:script_name, :redis_server_url)
  end
end

