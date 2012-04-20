ENV['RACK_ENV'] = 'test'
require 'spec_helper'
require 'yaml'
require 'qless/server'
require 'capybara/rspec'

module Qless
  describe Server, :type => :request do
    before(:all) do
      Qless::Server.client = Qless::Client.new(redis_config)
      Capybara.app = Qless::Server.new
    end

    it 'can visit each top-nav tab' do
      visit '/'

      links = all('ul.nav a')
      links.should have_at_least(6).links
      links.each do |link|
        click_link link.text
      end
    end
  end
end
