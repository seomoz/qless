require 'qless/lua_script'

module Qless
  class LuaScriptCache
    def initialize
      @cache = {}
    end

    def script_for(script_name, redis_connection)
      key = CacheKey.new(script_name, redis_connection.id)

      @cache.fetch(key) do
        @cache[key] = LuaScript.new(script_name, redis_connection)
      end
    end

    CacheKey = Struct.new(:script_name, :redis_server_url)
  end
end

