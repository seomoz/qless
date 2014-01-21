# Encoding: utf-8

begin
  # use `bundle install --standalone' to get this...
  require_relative '../bundle/bundler/setup'
rescue LoadError
  # fall back to regular bundler if the developer hasn't bundled standalone
  require 'bundler'
  Bundler.setup
end

require 'rspec/fire'

module QlessSpecHelpers
  def with_env_vars(vars)
    original = ENV.to_hash
    vars.each { |k, v| ENV[k] = v }

    begin
      yield
    ensure
      ENV.replace(original)
    end
  end

  def clear_qless_memoization
    Qless.instance_eval do
      instance_variables.each do |ivar|
        remove_instance_variable(ivar)
      end
    end
  end
end

require 'yaml'

module RedisHelpers
  extend self

  def redis_config
    return @redis_config unless @redis_config.nil?
    if File.exist?('./spec/redis.config.yml')
      @redis_config = YAML.load_file('./spec/redis.config.yml')
    else
      @redis_config = {}
    end
  end

  def redis_url
    return 'redis://localhost:6379/0' if redis_config.empty?
    c = redis_config
    "redis://#{c[:host]}:#{c[:port]}/#{c.fetch(:db, 0)}"
  end

  def new_client
    Qless::Client.new(redis_config)
  end

  def new_redis
    Redis.new(redis_config)
  end

  def new_redis_for_alternate_db
    config = redis_config.merge(db: redis_config.fetch(:db, 0) + 1)
    Redis.new(config)
  end
end

RSpec.configure do |c|
  c.treat_symbols_as_metadata_keys_with_true_values = true
  c.filter_run :f
  c.run_all_when_everything_filtered = true
  c.include RSpec::Fire
  c.include QlessSpecHelpers

  c.before(:each, :js) do
    pending 'Skipping JS test because JS tests have been flaky on Travis.'
  end if ENV['TRAVIS']
end

using_integration_context = false
shared_context 'redis integration', :integration do
  using_integration_context = true
  include RedisHelpers

  # A qless client subject to the redis configuration
  let(:client) { new_client }
  # A plain redis client with the same redis configuration
  let(:redis)  { new_redis }

  before(:each) { redis.script(:flush) }
  after(:each)  { redis.flushdb }
end

RSpec.configure do |c|
  c.before(:suite) do
    if using_integration_context && RedisHelpers.new_redis.keys('*').any?
      config = RedisHelpers.redis_config
      command = "redis-cli -h #{config.fetch(:host, "127.0.0.1")} -p #{config.fetch(:port, 6379)} -n #{config.fetch(:db, 0)} flushdb"
      msg = "Aborting since there are keys in your Redis DB and we don't want to accidentally clear data you may care about."
      msg << "  To clear your DB, run: `#{command}`"
      raise msg
    end
  end
end

# This context kills all the non-main threads and ensure they're cleaned up
shared_context 'stops all non-main threads', :uses_threads do
  after(:each) do
    # We're going to kill all the non-main threads
    threads = Thread.list - [Thread.main]
    threads.each(&:kill)
    threads.each(&:join)
  end
end
