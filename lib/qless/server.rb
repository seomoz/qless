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
          {:name => 'Config'  , :path => '/config'  },
          {:name => 'About'   , :path => '/about'   }
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
        # From http://stackoverflow.com/questions/195740/how-do-you-do-relative-time-in-rails
        diff_seconds = Time.now - t
        case diff_seconds
          when 0 .. 59
            "#{diff_seconds.to_i} seconds ago"
          when 60 .. (3600-1)
            "#{(diff_seconds/60).to_i} minutes ago"
          when 3600 .. (3600*24-1)
            "#{(diff_seconds/3600).to_i} hours ago"
          when (3600*24) .. (3600*24*30) 
            "#{(diff_seconds/(3600*24)).to_i} days ago"
          else
            start_time.strftime('%b %e, %Y %H:%M:%S %Z (%z)')
        end
      end
    end
    
    get '/?' do
      erb :overview, :layout => true, :locals => { :title => "Overview" }
    end
    
    get '/queues/?' do
      erb :queues, :layout => true, :locals => {
        :title   => 'Queues'
      }
    end
    
    get '/queues/:name/?:tab?' do
      queue = Server.client.queue(params[:name])
      tab    = params.fetch('tab', 'stats')
      jobs   = []
      case tab
      when 'running'
        jobs = queue.running
      when 'scheduled'
        jobs = queue.scheduled
      when 'stalled'
        jobs = queue.stalled
      end
      jobs = jobs.map { |jid| Server.client.job(jid) }
      if tab == 'waiting'
        jobs = queue.peek(20)
      end
      erb :queue, :layout => true, :locals => {
        :title   => "Queue #{params[:name]}",
        :tab     => tab,
        :jobs    => jobs,
        :queue   => Server.client.queues(params[:name]),
        :stats   => queue.stats
      }
    end
    
    get '/failed/?' do
      # qless-core doesn't provide functionality this way, so we'll
      # do it ourselves. I'm not sure if this is how the core library
      # should behave or not.
      erb :failed, :layout => true, :locals => {
        :title  => 'Failed',
        :failed => Server.client.failed.keys.map { |t| Server.client.failed(t).tap { |f| f['type'] = t } }
      }
    end
    
    get '/failed/:type/?' do
      erb :failed_type, :layout => true, :locals => {
        :title  => 'Failed | ' + params[:type],
        :type   => params[:type],
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
    
    get '/workers/?' do
      erb :workers, :layout => true, :locals => {
        :title   => 'Workers'
      }
    end

    get '/workers/:worker' do
      erb :worker, :layout => true, :locals => {
        :title  => 'Worker | ' + params[:worker],
        :worker => Server.client.workers(params[:worker]).tap { |w|
          w['jobs']    = w['jobs'].map { |j| Server.client.job(j) }
          w['stalled'] = w['stalled'].map { |j| Server.client.job(j) }
          w['name']    = params[:worker]
        }
      }
    end
    
    get '/config/?' do
      erb :config, :layout => true, :locals => {
        :title   => 'Config',
        :options => Server.client.config.all
      }
    end
    
    get '/about/?' do
      erb :about, :layout => true, :locals => {
        :title   => 'About'
      }
    end
    
    
    
    
    
    
    
    # These are the bits where we accept AJAX requests
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
    post "/retryall/?" do
      # Expects a JSON-encoded hash of type: failure-type
      data = JSON.parse(request.body.read)
      if data["type"].nil?
        halt 400, "Neet type"
      else
        return json(Server.client.failed(data["type"])['jobs'].map do |job|
          queue = job.history[-1]["q"]
          job.move(queue)
          { :id => job.id, :queue => queue}
        end)
      end
    end
    
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
    
    post "/cancelall/?" do
      # Expects a JSON-encoded hash of type: failure-type
      data = JSON.parse(request.body.read)
      if data["type"].nil?
        halt 400, "Neet type"
      else
        return json(Server.client.failed(data["type"])['jobs'].map do |job|
          job.cancel()
          { :id => job.id }
        end)
      end
    end
    
    # start the server if ruby file executed directly
    run! if app_file == $0
  end
end