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
      
      def failed
        return Server.client.failed
      end
      
      def json(obj)
        content_type :json
        obj.to_json
      end
      
      def strftime(t)
        return t.strftime('%b %e, %Y %H:%M:%S %Z (%z)')
      end
    end
    
    get '/?' do
      erb :overview, :layout => true, :locals => {}
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
    
    get '/failed/?' do
      erb :failed, :layout => true, :locals => {
        :title  => 'Failed'
      }
    end
    
    get '/failed/:type/?' do
      erb :failed_type, :layout => true, :locals => {
        :title  => 'Failed | ' + params[:type],
        :failed => Server.client.failed(params[:type])
      }
    end
    
    get '/track/?' do
      erb :track, :layout => true, :locals => {
        :title   => 'Track'
      }
    end
    
    get '/jobs/:jid' do
      erb :job, :layout => true, :locals => {
        :title => "Job | #{params[:jid]}",
        :jid   => params[:jid],
        :job   => Server.client.job(params[:jid])
      }
    end
    
    # These are the bits where we accept AJAX requests
    post "/cancel/?" do
      # Expects a JSON-encoded array of job ids to cancel
      jobs = JSON.parse(request.body.read).map { |jid| Server.client.job(jid) }.select { |j| not j.nil? }
      # Go ahead and cancel all the jobs!
      jobs.each do |job|
        job.cancel()
      end
      
      if request.xhr?
        return json({ :canceled => jobs.map { |job| job.id } })
      else
        redirect to(request.referrer)
      end
    end
    
    post "/track/?" do
      # Expects a JSON-encoded hash with a job id, and optionally some tags
      data = JSON.parse(request.body.read)
      job = Server.client.job(data["id"])
      if not job.nil?
        data.fetch("tags", false) ? job.track(*data["tags"]) : job.track()
        if request.xhr?
          json({ :tracked => [job.id] })
        else
          redirect to('/track')
        end
      else
        if request.xhr?
          json({ :tracked => [] })
        else
          redirect to(request.referrer)
        end
      end
    end
    
    post "/untrack/?" do
      # Expects a JSON-encoded array of job ids to stop tracking
      jobs = JSON.parse(request.body.read).map { |jid| Server.client.job(jid) }.select { |j| not j.nil? }
      # Go ahead and cancel all the jobs!
      jobs.each do |job|
        job.untrack()
      end
      
      return json({ :untracked => jobs.map { |job| job.id } })
    end
    
    post "/move/?" do
      # Expects a JSON-encoded hash of id: jid, and queue: queue_name
      data = JSON.parse(request.body.read)
      if data["id"].nil? or data["queue"].nil?
        halt 400, "Need id and queue arguments"
      else
        job = Server.client.job(data["id"])
        if job.nil?
          halt 404, "Could not find job"
        else
          job.move(data["queue"])
          return json({ :id => data["id"], :queue => data["queue"]})
        end
      end
    end
    
    post "/retry/?" do
      # Expects a JSON-encoded hash of id: jid, and queue: queue_name
      data = JSON.parse(request.body.read)
      if data["id"].nil?
        halt 400, "Need id"
      else
        job = Server.client.job(data["id"])
        if job.nil?
          halt 404, "Could not find job"
        else
          queue = job.history[-1]["q"]
          job.move(queue)
          return json({ :id => data["id"], :queue => queue})
        end
      end
    end
    
    # Retry all the failures of a particular type
    post "/retry-type/?" do
      # Expects a JSON-encoded hash of type: failure-type
      data = JSON.parse(request.body.read)
    end
    
    
    
    
    
    
    
    
    
    
    
    

    get '/config/?' do
      erb :config, :layout => true, :locals => {
        :title   => 'Config',
        :options => Server.client.config.all
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

    get '/complete/?' do
    end
    
    # start the server if ruby file executed directly
    run! if app_file == $0
  end
end