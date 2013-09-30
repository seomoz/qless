# Encoding: utf-8

require 'digest/sha1'

module Qless
  LuaScriptError = Class.new(Qless::Error)

  # Wraps a lua script. Knows how to reload it if necessary
  class LuaScript
    SCRIPT_ROOT = File.expand_path('../lua', __FILE__)

    def initialize(name, redis)
      @name  = name
      @redis = redis
      @sha   = Digest::SHA1.hexdigest(script_contents)
    end

    attr_reader :name, :redis, :sha

    def reload
      @sha = @redis.script(:load, script_contents)
    end

    def call(*argv)
      handle_no_script_error do
        _call(*argv)
      end
    rescue Redis::CommandError => err
      if match = err.message.match('user_script:\d+:\s*(\w+.+$)')
        raise LuaScriptError.new(match[1])
      else
        raise err
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

    def handle_no_script_error
      yield
    rescue ScriptNotLoadedRedisCommandError
      reload
      yield
    end

    # Module for notifying when a script hasn't yet been loaded
    module ScriptNotLoadedRedisCommandError
      MESSAGE = 'NOSCRIPT No matching script. Please use EVAL.'

      def self.===(error)
        error.is_a?(Redis::CommandError) && error.message == MESSAGE
      end
    end

    def script_contents
      @script_contents ||= File.read(File.join(SCRIPT_ROOT, "#{@name}.lua"))
    end
  end

  # Provides a simple way to load and use lua-based Qless plugins.
  # This combines the qless-lib.lua script plus your custom script
  # contents all into one script, so that your script can use
  # Qless's lua API.
  class LuaPlugin < LuaScript
    def initialize(name, redis, plugin_contents)
      @name  = name
      @redis = redis
      @plugin_contents = plugin_contents.gsub(COMMENT_LINES_RE, '')
      super(name, redis)
    end

  private

    def script_contents
      @script_contents ||= [QLESS_LIB_CONTENTS, @plugin_contents].join("\n\n")
    end

    COMMENT_LINES_RE = /^\s*--.*$\n?/

    QLESS_LIB_CONTENTS = File.read(
      File.join(SCRIPT_ROOT, 'qless-lib.lua')
    ).gsub(COMMENT_LINES_RE, '')
  end
end
