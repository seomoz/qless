begin
  # use `bundle install --standalone' to get this...
  require_relative '../bundle/bundler/setup'
rescue LoadError
  # fall back to regular bundler if the developer hasn't bundled standalone
  require 'bundler'
  Bundler.setup
end

require 'rspec/fire'

RSpec.configure do |c|
  c.treat_symbols_as_metadata_keys_with_true_values = true
  c.filter_run :f
  c.run_all_when_everything_filtered = true
  c.include RSpec::Fire
end

shared_context "redis integration", :integration do
  let(:client) { Qless::Client.new(redis_config) }
  let(:redis_config) do
    if File.exist?('./spec/redis.config.yml')
      YAML.load_file('./spec/redis.config.yml')
    else
      {}
    end
  end

  def assert_minimum_redis_version(version)
    redis_version = Gem::Version.new(@redis.info["redis_version"])
    if redis_version < Gem::Version.new(version)
      pending "You are running redis #{redis_version}, but qless requires at least #{version}"
    end
  end

  before(:each) do
    # Sometimes we need raw redis access
    @redis = Redis.new(redis_config)
    assert_minimum_redis_version("2.6")
    if @redis.keys("*").length > 0
      raise "Must start with empty Redis DB"
    end
    @redis.script(:flush)
  end

  after(:each) do
    @redis.flushdb
  end
end


