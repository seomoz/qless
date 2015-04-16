# Encoding: utf-8

require 'sinatra/base'
require 'qless'

module Qless
  # The Qless web interface
  class Server < Sinatra::Base
    # Path-y-ness
    dir = File.dirname(File.expand_path(__FILE__))
    set :views        , "#{dir}/server/views"
    set :public_folder, "#{dir}/server/static"

    # For debugging purposes at least, I want this
    set :reload_templates, true

    # I'm not sure what this option is -- I'll look it up later
    # set :static, true

    attr_reader :client

    def initialize(client)
      @client = client
      super
    end

    helpers do
      include Rack::Utils

      def url_path(*path_parts)
        [path_prefix, path_parts].join('/').squeeze('/')
      end
      alias_method :u, :url_path

      def path_prefix
        request.env['SCRIPT_NAME']
      end

      def url_with_modified_query
        url = URI(request.url)
        existing_query = Rack::Utils.parse_query(url.query)
        url.query = Rack::Utils.build_query(yield existing_query)
        url.to_s
      end

      def page_url(offset)
        url_with_modified_query do |query|
          query.merge('page' => current_page + offset)
        end
      end

      def next_page_url
        page_url(1)
      end

      def prev_page_url
        page_url(-1)
      end

      def current_page
        @current_page ||= begin
          Integer(params[:page])
        rescue
          1
        end
      end

      PAGE_SIZE = 25
      def pagination_values
        start = (current_page - 1) * PAGE_SIZE
        [start, start + PAGE_SIZE]
      end

      def paginated(qless_object, method, *args)
        qless_object.send(method, *(args + pagination_values))
      end

      def tabs
        [
          { name: 'Queues'   , path: '/queues'   },
          { name: 'Workers'  , path: '/workers'  },
          { name: 'Track'    , path: '/track'    },
          { name: 'Failed'   , path: '/failed'   },
          { name: 'Completed', path: '/completed'},
          { name: 'Config'   , path: '/config'   },
          { name: 'About'    , path: '/about'    }
        ]
      end

      def application_name
        client.config['application']
      end

      def queues
        client.queues.counts
      end

      def tracked
        client.jobs.tracked
      end

      def workers
        client.workers.counts
      end

      def failed
        client.jobs.failed
      end

      # Return the supplied object back as JSON
      def json(obj)
        content_type :json
        obj.to_json
      end

      # Make the id acceptable as an id / att in HTML
      def sanitize_attr(attr)
        attr.gsub(/[^a-zA-Z\:\_]/, '-')
      end

      # What are the top tags? Since it might go on, say, every
      # page, then we should probably be caching it
      def top_tags
        @top_tags ||= {
          top: client.tags,
          fetched: Time.now
        }
        if (Time.now - @top_tags[:fetched]) > 60
          @top_tags = {
            top: client.tags,
            fetched: Time.now
          }
        end
        @top_tags[:top]
      end

      def strftime(t)
        # From http://stackoverflow.com/questions/195740
        diff_seconds = Time.now - t
        formatted = t.strftime('%b %e, %Y %H:%M:%S')
        case diff_seconds
        when 0 .. 59
          "#{formatted} (#{diff_seconds.to_i} seconds ago)"
        when 60 ... 3600
          "#{formatted} (#{(diff_seconds / 60).to_i} minutes ago)"
        when 3600 ... 3600 * 24
          "#{formatted} (#{(diff_seconds / 3600).to_i} hours ago)"
        when (3600 * 24) ... (3600 * 24 * 30)
          "#{formatted} (#{(diff_seconds / (3600 * 24)).to_i} days ago)"
        else
          formatted
        end
      end
    end

    get '/?' do
      erb :overview, layout: true, locals: { title: 'Overview' }
    end

    # Returns a JSON blob with the job counts for various queues
    get '/queues.json' do
      json(client.queues.counts)
    end

    get '/queues/?' do
      erb :queues, layout: true, locals: {
        title: 'Queues'
      }
    end

    # Return the job counts for a specific queue
    get '/queues/:name.json' do
      json(client.queues[params[:name]].counts)
    end

    filtered_tabs = %w[ running scheduled stalled depends recurring ].to_set
    get '/queues/:name/?:tab?' do
      queue = client.queues[params[:name]]
      tab   = params.fetch('tab', 'stats')

      jobs = []
      if tab == 'waiting'
        jobs = queue.peek(20)
      elsif filtered_tabs.include?(tab)
        jobs = paginated(queue.jobs, tab).map { |jid| client.jobs[jid] }
      end

      erb :queue, layout: true, locals: {
        title: "Queue #{params[:name]}",
        tab: tab,
        jobs: jobs,
        queue: client.queues[params[:name]].counts,
        stats: queue.stats
      }
    end

    get '/failed.json' do
      json(client.jobs.failed)
    end

    get '/failed/?' do
      # qless-core doesn't provide functionality this way, so we'll
      # do it ourselves. I'm not sure if this is how the core library
      # should behave or not.
      erb :failed, layout: true, locals: {
        title: 'Failed',
        failed: client.jobs.failed.keys.map do |t|
          client.jobs.failed(t).tap { |f| f['type'] = t }
        end
      }
    end

    get '/failed/:type/?' do
      erb :failed_type, layout: true, locals: {
        title: 'Failed | ' + params[:type],
        type: params[:type],
        failed: paginated(client.jobs, :failed, params[:type])
      }
    end

    get '/completed/?' do
      completed = paginated(client.jobs, :complete)
      erb :completed, layout: true, locals: {
        title: 'Completed',
        jobs: completed.map { |jid| client.jobs[jid] }
      }
    end

    get '/track/?' do
      erb :track, layout: true, locals: {
        title: 'Track'
      }
    end

    get '/jobs/:jid' do
      erb :job, layout: true, locals: {
        title: "Job | #{params[:jid]}",
        jid: params[:jid],
        job: client.jobs[params[:jid]]
      }
    end

    get '/workers/?' do
      erb :workers, layout: true, locals: {
        title: 'Workers'
      }
    end

    get '/workers/:worker' do
      erb :worker, layout: true, locals: {
        title: 'Worker | ' + params[:worker],
        worker: client.workers[params[:worker]].tap do |w|
          w['jobs']    = w['jobs'].map { |j| client.jobs[j] }
          w['stalled'] = w['stalled'].map { |j| client.jobs[j] }
          w['name']    = params[:worker]
        end
      }
    end

    get '/tag/?' do
      jobs = paginated(client.jobs, :tagged, params[:tag])
      erb :tag, layout: true, locals: {
        title: "Tag | #{params[:tag]}",
        tag: params[:tag],
        jobs: jobs['jobs'].map { |jid| client.jobs[jid] },
        total: jobs['total']
      }
    end

    get '/config/?' do
      erb :config, layout: true, locals: {
        title: 'Config',
        options: client.config.all
      }
    end

    get '/about/?' do
      erb :about, layout: true, locals: {
        title: 'About'
      }
    end

    # These are the bits where we accept AJAX requests
    post '/track/?' do
      # Expects a JSON-encoded hash with a job id, and optionally some tags
      data = JSON.parse(request.body.read)
      job = client.jobs[data['id']]
      if !job.nil?
        data.fetch('tags', false) ? job.track(*data['tags']) : job.track
        if request.xhr?
          json({ tracked: [job.jid] })
        else
          redirect to('/track')
        end
      else
        if request.xhr?
          json({ tracked: [] })
        else
          redirect to(request.referrer)
        end
      end
    end

    post '/untrack/?' do
      # Expects a JSON-encoded array of job ids to stop tracking
      jobs = JSON.parse(request.body.read).map { |jid| client.jobs[jid] }
      jobs.compact!
      # Go ahead and cancel all the jobs!
      jobs.each do |job|
        job.untrack
      end
      return json({ untracked: jobs.map { |job| job.jid } })
    end

    post '/priority/?' do
      # Expects a JSON-encoded dictionary of jid => priority
      response = Hash.new
      r = JSON.parse(request.body.read)
      r.each_pair do |jid, priority|
        begin
          client.jobs[jid].priority = priority
          response[jid] = priority
        rescue
          response[jid] = 'failed'
        end
      end
      return json(response)
    end

    post '/pause/?' do
      # Expects JSON blob: {'queue': <queue>}
      r = JSON.parse(request.body.read)
      if r['queue']
        @client.queues[r['queue']].pause
        return json({ queue: 'paused' })
      else
        raise 'No queue provided'
      end
    end

    post '/unpause/?' do
      # Expects JSON blob: {'queue': <queue>}
      r = JSON.parse(request.body.read)
      if r['queue']
        @client.queues[r['queue']].unpause
        return json({ queue: 'unpaused' })
      else
        raise 'No queue provided'
      end
    end

    post '/timeout/?' do
      # Expects JSON blob: {'jid': <jid>}
      r = JSON.parse(request.body.read)
      if r['jid']
        @client.jobs[r['jid']].timeout
        return json({ jid: r['jid'] })
      else
        raise 'No jid provided'
      end
    end

    post '/tag/?' do
      # Expects a JSON-encoded dictionary of jid => [tag, tag, tag]
      response = Hash.new
      JSON.parse(request.body.read).each_pair do |jid, tags|
        begin
          client.jobs[jid].tag(*tags)
          response[jid] = tags
        rescue
          response[jid] = 'failed'
        end
      end
      return json(response)
    end

    post '/untag/?' do
      # Expects a JSON-encoded dictionary of jid => [tag, tag, tag]
      response = Hash.new
      JSON.parse(request.body.read).each_pair do |jid, tags|
        begin
          client.jobs[jid].untag(*tags)
          response[jid] = tags
        rescue
          response[jid] = 'failed'
        end
      end
      return json(response)
    end

    post '/move/?' do
      # Expects a JSON-encoded hash of id: jid, and queue: queue_name
      data = JSON.parse(request.body.read)
      if data['id'].nil? || data['queue'].nil?
        halt 400, 'Need id and queue arguments'
      else
        job = client.jobs[data['id']]
        if job.nil?
          halt 404, 'Could not find job'
        else
          job.requeue(data['queue'])
          return json({ id: data['id'], queue: data['queue'] })
        end
      end
    end

    post '/undepend/?' do
      # Expects a JSON-encoded hash of id: jid, and queue: queue_name
      data = JSON.parse(request.body.read)
      if data['id'].nil?
        halt 400, 'Need id'
      else
        job = client.jobs[data['id']]
        if job.nil?
          halt 404, 'Could not find job'
        else
          job.undepend(data['dependency'])
          return json({ id: data['id'] })
        end
      end
    end

    post '/retry/?' do
      # Expects a JSON-encoded hash of id: jid, and queue: queue_name
      data = JSON.parse(request.body.read)
      if data['id'].nil?
        halt 400, 'Need id'
      else
        job = client.jobs[data['id']]
        if job.nil?
          halt 404, 'Could not find job'
        else
          job.requeue(job.queue_name)
          return json({ id: data['id'], queue: job.queue_name })
        end
      end
    end

    # Retry all the failures of a particular type
    post '/retryall/?' do
      # Expects a JSON-encoded hash of type: failure-type
      data = JSON.parse(request.body.read)
      if data['type'].nil?
        halt 400, 'Neet type'
      else
        jobs = client.jobs.failed(data['type'], 0, 500)['jobs']
        results = jobs.map do |job|
          job.requeue(job.queue_name)
          { id: job.jid, queue: job.queue_name }
        end
        return json(results)
      end
    end

    post '/cancel/?' do
      # Expects a JSON-encoded array of job ids to cancel
      jobs = JSON.parse(request.body.read).map { |jid| client.jobs[jid] }
      jobs.compact!
      # Go ahead and cancel all the jobs!
      jobs.each do |job|
        job.cancel
      end

      if request.xhr?
        return json({ canceled: jobs.map { |job| job.jid } })
      else
        redirect to(request.referrer)
      end
    end

    post '/cancelall/?' do
      # Expects a JSON-encoded hash of type: failure-type
      data = JSON.parse(request.body.read)
      if data['type'].nil?
        halt 400, 'Neet type'
      else
        return json(client.jobs.failed(data['type'])['jobs'].map do |job|
          job.cancel
          { id: job.jid }
        end)
      end
    end

    # start the server if ruby file executed directly
    run! if app_file == $PROGRAM_NAME
  end
end
