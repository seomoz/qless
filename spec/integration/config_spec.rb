# Encoding: utf-8

# The things we're testing
require 'qless'

# Spec stuff
require 'spec_helper'

module Qless
  describe Config, :integration do
    it 'can set, get and erase configuration' do
      client.config['testing'] = 'foo'
      client.config['testing'].should eq('foo')
      client.config.all['testing'].should eq('foo')
      client.config.clear('testing')
      client.config['testing'].should eq(nil)
    end

    it 'can get all configurations' do
      expect(client.config.all).to eq({
        'heartbeat'          => 60,
        'application'        => 'qless',
        'grace-period'       => 10,
        'jobs-history'       => 604800,
        'stats-history'      => 30,
        'histogram-history'  => 7,
        'jobs-history-count' => 50000
      })
    end
  end
end
