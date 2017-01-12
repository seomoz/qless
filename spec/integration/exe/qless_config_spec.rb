# Encoding: utf-8

require 'json'
require 'tempfile'

require 'spec_helper'

describe 'qless-config', :integration do
  let(:path) { './exe/qless-config' }

  let(:default) do
    {
      'application' => 'qless',
      'grace-period' => 10,
      'stats-history' => 30,
      'jobs-history' => 604800,
      'heartbeat' => 60,
      'jobs-history-count' => 50000,
      'histogram-history' => 7
    }
  end

  def options_fixture(options = {})
    Tempfile.open('fixture') do |f|
      JSON.dump(options, f)
      f.flush
      yield f.path
    end
  end

  def run(*args)
    `#{path} #{args.join(' ')}`
  end

  it 'runs dump' do
    expect(JSON.parse(run('dump'))).to eq(default)
  end

  it 'runs load' do
    options_fixture(:foo => 'bar') do |path|
      run('load', path)
    end
    expect(client.config['foo']).to eq('bar')
  end

  it 'can clear variables' do
    client.config['foo'] = 'bar'
    options_fixture do |path|
      run('load', path, '--clear')
    end
    expect(client.config.all).to eq(default)
  end
end
