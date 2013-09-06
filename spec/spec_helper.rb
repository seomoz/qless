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

  def redis_config
    @redis_config ||= if File.exist?('./spec/redis.config.yml')
      YAML.load_file('./spec/redis.config.yml')
    else
      {}
    end
  end

  def redis_url
    return "redis://localhost:6379/0" if redis_config.empty?
    "redis://#{redis_config[:host]}:#{redis_config[:port]}/#{redis_config.fetch(:db, 0)}"
  end

  def clear_qless_memoization
    Qless.instance_eval do
      instance_variables.each do |ivar|
        remove_instance_variable(ivar)
      end
    end
  end
end

RSpec.configure do |c|
  c.treat_symbols_as_metadata_keys_with_true_values = true
  c.filter_run :f
  c.run_all_when_everything_filtered = true
  c.include RSpec::Fire
  c.include QlessSpecHelpers

  c.before(:each, :js) do
    pending "Skipping JS test because JS tests have been flaky on Travis."
  end if ENV['TRAVIS']
end

shared_context "redis integration", :integration do
  def new_client
    Qless::Client.new(redis_config)
  end

  let(:client) { new_client }

  before(:each) do
    # Sometimes we need raw redis access
    @redis = Redis.new(redis_config)
    if @redis.keys("*").length > 0
      pending "Must start with empty Redis DB, but had keys: #{@redis.keys("*").inspect}"
    end
    @redis.script(:flush)
  end

  after(:each) do
    @redis && @redis.flushdb
  end
end

shared_context "stops all non-main threads", :uses_threads do
  require 'qless/wait_until'

  def non_main_threads
    Thread.list - [Thread.main]
  end

  after(:each) do
    threads_to_kill = self.non_main_threads
    threads_to_kill.each(&:kill)
    Qless::WaitUntil.wait_until(2) { non_main_threads.empty? }
  end
end

