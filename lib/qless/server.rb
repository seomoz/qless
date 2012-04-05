#! /usr/bin/env ruby

require 'sinatra/base'
require 'qless'

# Much of this is shamelessly poached from the resque web client

client = Qless::Client.new

module Qless
  class Server < Sinatra::Base
    # Path-y-ness
    dir = File.dirname(File.expand_path(__FILE__))
    set :views        , "#{dir}/server/views"
    set :public_folder, "#{dir}/server/static"
    
    # For debugging purposes at least, I want this
    set :reload_templates, true
    
    # I'm not sure what this option is -- I'll look it up later
    # set :static, true
    
    def self.client
      @client ||= Qless::Client.new
    end
    
    helpers do
      def tabs
        return [
          {:name => 'Overview', :path => '/'        },
          {:name => 'Workers' , :path => '/workers' },
          {:name => 'Queues'  , :path => '/queues'  },
          {:name => 'Track'   , :path => '/track'   },
          {:name => 'Failed'  , :path => '/failed'  },
          {:name => 'Complete', :path => '/complete'},
          {:name => 'Config'  , :path => '/config'  }
        ]
      end
      
      def queues
        return Server.client.queues
      end
      
      def tracked
        return Server.client.tracked
      end
      
      def workers
        return Server.client.workers
      end
    end
    
    get '/queues/?' do
      erb :queues, :layout => true, :locals => {
        :title   => 'Queues'
      }
    end
    
    get '/queues/:name' do
      erb :queue, :layout => true, :locals => {
        :title   => 'Queue "#{params[:name]}"',
        :queue   => Server.client.queues(params[:name]),
        :stats   => Server.client.queue(params[:name]).stats
      }
    end
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    get '/?' do
      erb :overview, :layout => true, :locals => {
        :title  => 'Hello!'
      }
    end

    get '/track/?' do
      erb :track, :layout => true, :locals => {
        :title   => 'Track'
      }
    end

    get '/config/?' do
      erb :config, :layout => true, :locals => {
        :title   => 'Config',
        :options => Server.client.config.all
      }
    end

    get '/queue/:name' do
      erb :queue, :layout => true, :locals => {
        :title => 'Queue | ' + params[:name],
        :tabs  => tabs,
        :stats => Server.client.queue(params[:name]).stats
      }
    end

    get '/workers/?' do
      erb :workers, :layout => true, :locals => {
        :title   => 'Workers'
      }
    end

    get '/workers/:worker' do
      erb :worker, :layout => true, :locals => {
        :title  => 'Worker | ' + params[:worker],
        :worker => client.workers(params[:worker])
      }
    end

    get '/jobs/:jid' do
      erb :job, :layout => true, :locals => {
        :title => 'Job | ' + params[:jid],
        :job   => client.job(params[:jid])
      }
    end

    get '/failed/?' do
      erb :failed, :layout => true, :locals => {
        :title  => 'Failed',
        :failed => client.failed()
      }
    end

    get '/failed/:type' do
      erb :failed_type, :layout => true, :locals => {
        :title  => 'Failed | ' + params[:type],
        :failed => client.failed(params[:type])
      }
    end

    get '/complete/?' do
    end
    
    # start the server if ruby file executed directly
    run! if app_file == $0
  end
end