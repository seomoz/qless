require 'qless'
require 'time'
require 'qless/job_reservers/ordered'
require 'qless/job_reservers/round_robin'

module Qless
  # This is heavily inspired by Resque's excellent worker:
  # https://github.com/defunkt/resque/blob/v1.20.0/lib/resque/worker.rb
  class Worker
    def initialize(job_reserver, options = {})
      self.job_reserver = job_reserver
      @shutdown = @paused = false

      self.very_verbose = options[:very_verbose]
      self.verbose = options[:verbose]
      self.run_as_single_process = options[:run_as_single_process]
      self.output = options.fetch(:output, $stdout)

      output.puts "\n\n\n" if verbose || very_verbose
      log "Instantiated Worker"
    end

    # Whether the worker should log basic info to STDOUT
    attr_accessor :verbose

    # Whether the worker should log lots of info to STDOUT
    attr_accessor  :very_verbose

    # Whether the worker should run in a single prcoess
    # i.e. not fork a child process to do the work
    # This should only be true in a dev/test environment
    attr_accessor :run_as_single_process

    # An IO-like object that logging output is sent to.
    # Defaults to $stdout.
    attr_accessor :output

    # The object responsible for reserving jobs from the Qless server,
    # using some reasonable strategy (e.g. round robin or ordered)
    attr_accessor :job_reserver

    # Starts a worker based on ENV vars. Supported ENV vars:
    #   - REDIS_URL=redis://host:port/db-num (the redis gem uses this automatically)
    #   - QUEUES=high,medium,low or QUEUE=blah
    #   - JOB_RESERVER=Ordered or JOB_RESERVER=RoundRobin
    #   - INTERVAL=3.2
    #   - VERBOSE=true (to enable logging)
    #   - VVERBOSE=true (to enable very verbose logging)
    #   - RUN_AS_SINGLE_PROCESS=true (false will fork children to do work, true will keep it single process)
    # This is designed to be called from a rake task
    def self.start
      client = Qless::Client.new
      queues = (ENV['QUEUES'] || ENV['QUEUE']).to_s.split(',').map { |q| client.queues[q.strip] }
      if queues.none?
        raise "No queues provided. You must pass QUEUE or QUEUES when starting a worker."
      end

      reserver = JobReservers.const_get(ENV.fetch('JOB_RESERVER', 'Ordered')).new(queues)
      interval = Float(ENV.fetch('INTERVAL', 5.0))

      options = {}
      options[:verbose] = !!ENV['VERBOSE']
      options[:very_verbose] = !!ENV['VVERBOSE']
      options[:run_as_single_process] = !!ENV['RUN_AS_SINGLE_PROCESS']

      new(reserver, options).work(interval)
    end

    def work(interval = 5.0)
      procline "Starting #{@job_reserver.description}"
      register_signal_handlers

      loop do
        break if shutdown?
        if paused?
          sleep interval
          next
        end

        unless job = @job_reserver.reserve
          break if interval.zero?
          procline "Waiting for #{@job_reserver.description}"
          log! "Sleeping for #{interval} seconds"
          sleep interval
          next
        end

        log "got: #{job.inspect}"

        if run_as_single_process
          # We're staying in the same process
          procline "Single processing #{job.description}"
          perform(job)
        elsif @child = fork
          # We're in the parent process
          procline "Forked #{@child} for #{job.description}"
          Process.wait(@child)
        else
          # We're in the child process
          procline "Processing #{job.description}"
          job.reconnect_to_redis
          perform(job)
          exit!
        end
      end
    end

    def perform(job)
      around_perform(job)
    rescue *retryable_exception_classes(job)
      job.retry
    rescue Exception => error
      fail_job(job, error)
    else
      try_complete(job)
    end

    def shutdown
      @shutdown = true
    end

    def shutdown!
      shutdown
      kill_child unless run_as_single_process
    end

    def shutdown?
      @shutdown
    end

    def paused?
      @paused
    end

    def pause_processing
      log "USR2 received; pausing job processing"
      @paused = true
      procline "Paused -- #{@job_reserver.description}"
    end

    def unpause_processing
      log "CONT received; resuming job processing"
      @paused = false
    end

  private

    def retryable_exception_classes(job)
      return [] unless job.klass.respond_to?(:retryable_exception_classes)
      job.klass.retryable_exception_classes
    rescue NameError => exn
      []
    end

    def try_complete(job)
      job.complete unless job.state_changed?
    rescue Job::CantCompleteError => e
      # There's not much we can do here. Complete fails in a few cases:
      #   - The job is already failed (i.e. by another worker)
      #   - The job is being worked on by another worker
      #   - The job has been cancelled
      #
      # We don't want to (or are able to) fail the job with this error in
      # any of these cases, so the best we can do is log the failure.
      log "Failed to complete #{job.inspect}: #{e.message}"
    end

    # Allow middleware modules to be mixed in and override the
    # definition of around_perform while providing a default
    # implementation so our code can assume the method is present.
    include Module.new {
      def around_perform(job)
        job.perform
      end
    }

    def fail_job(job, error)
      group = "#{job.klass_name}:#{error.class}"
      message = "#{error.message}\n\n#{error.backtrace.join("\n")}"
      log "Got #{group} failure from #{job.inspect}"
      job.fail(group, message)
    end

    def procline(value)
      $0 = "Qless-#{Qless::VERSION}: #{value} at #{Time.now.iso8601}"
      log! $0
    end

    def kill_child
      return unless @child
      return unless system("ps -o pid,state -p #{@child}")
      Process.kill("KILL", @child) rescue nil
    end

    # This is stolen directly from resque... (thanks, @defunkt!)
    # Registers the various signal handlers a worker responds to.
    #
    # TERM: Shutdown immediately, stop processing jobs.
    #  INT: Shutdown immediately, stop processing jobs.
    # QUIT: Shutdown after the current job has finished processing.
    # USR1: Kill the forked child immediately, continue processing jobs.
    # USR2: Don't process any new jobs
    # CONT: Start processing jobs again after a USR2
    def register_signal_handlers
      trap('TERM') { shutdown!  }
      trap('INT')  { shutdown!  }

      begin
        trap('QUIT') { shutdown   }
        trap('USR1') { kill_child }
        trap('USR2') { pause_processing }
        trap('CONT') { unpause_processing }
      rescue ArgumentError
        warn "Signals QUIT, USR1, USR2, and/or CONT not supported."
      end
    end

    # Log a message to STDOUT if we are verbose or very_verbose.
    def log(message)
      if verbose
        output.puts "*** #{message}"
      elsif very_verbose
        time = Time.now.strftime('%H:%M:%S %Y-%m-%d')
        output.puts "** [#{time}] #$$: #{message}"
      end
    end

    # Logs a very verbose message to STDOUT.
    def log!(message)
      log message if very_verbose
    end
  end
end

