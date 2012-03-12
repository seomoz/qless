#! /usr/bin/env ruby

require 'sinatra'

set :public_folder, File.dirname(__FILE__) + '/static'
set :views, File.dirname(__FILE__) + '/views'
set :reload_templates, true

require 'qless'

client = Qless::Client.new

tabs = [
  {:name => 'Overview', :path => '/'        },
  {:name => 'Workers' , :path => '/workers' },
  {:name => 'Queues'  , :path => '/queues'  },
  {:name => 'Track'   , :path => '/track'   },
  {:name => 'Failed'  , :path => '/failed'  },
  {:name => 'Complete', :path => '/complete'},
  {:name => 'Config'  , :path => '/config'  }
]

get '/?' do
  erb :overview, :layout => true, :locals => {
    :title  => 'Hello!',
    :tabs   => tabs,
    :queues => client.queues
  }
end

get '/track/?' do
  erb :track, :layout => true, :locals => {
    :title   => 'Track',
    :tabs    => tabs,
    :tracked => client.tracked
  }
end

get '/config/?' do
  erb :config, :layout => true, :locals => {
    :title   => 'Config',
    :tabs    => tabs,
    :options => client.config.get
  }
end

get '/queues/?' do
  erb :queues, :layout => true, :locals => {
    :title   => 'Queues',
    :tabs    => tabs,
    :queues  => client.queues
  }
end

get '/queue/:name' do
  erb :queue, :layout => true, :locals => {
    :title => 'Queue | ' + params[:name],
    :tabs  => tabs,
    :stats => client.queue(params[:name]).stats
  }
end

get '/workers/?' do
  erb :workers, :layout => true, :locals => {
    :title   => 'Workers',
    :tabs    => tabs,
    :workers => client.workers
  }
end

get '/workers/:worker' do
  erb :worker, :layout => true, :locals => {
    :title  => 'Worker | ' + params[:worker],
    :tabs   => tabs,
    :worker => client.workers(params[:worker])
  }
end

get '/jobs/:jid' do
  erb :job, :layout => true, :locals => {
    :title => 'Job | ' + params[:jid],
    :tabs  => tabs,
    :job   => client.job(params[:jid])
  }
end

get '/failed/?' do
  erb :failed, :layout => true, :locals => {
    :title  => 'Failed',
    :tabs   => tabs,
    :failed => client.failed()
  }
end

get '/failed/:type' do
  erb :failed_type, :layout => true, :locals => {
    :title  => 'Failed | ' + params[:type],
    :tabs   => tabs,
    :failed => client.failed(params[:type])
  }
end

get '/complete/?' do
end