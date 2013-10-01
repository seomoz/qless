# Standard stuff
require 'time'
require 'logger'

# Qless requires
require 'qless'
require 'qless/job_reservers/ordered'
require 'qless/job_reservers/round_robin'
require 'qless/job_reservers/shuffled_round_robin'
require 'qless/subscriber'
require 'qless/wait_until'

module Qless
  JobLockLost = Class.new(StandardError)

  class BaseWorker
    # An IO-like object that logging output is sent to.
    # Defaults to $stdout.
    attr_accessor :output

    # The object responsible for reserving jobs from the Qless server,
    # using some reasonable strategy (e.g. round robin or ordered)
    attr_accessor :reserver

    # The logging level
    attr_accessor :log_level

    # The polling interval
    attr_accessor :interval

    # The child startup interval
    attr_accessor :max_startup_delay

    # The paused
    attr_accessor :paused

    def initialize(reserver, options = {})
      # Our job reserver
      @reserver = reserver

      # Our logger
      @log = Logger.new(options[:output] || $stdout)
      @log_level = options[:log_level] || Logger::WARN
      @log.level = @log_level
      @log.formatter = proc do |severity, datetime, progname, msg|
        "#{datetime}: #{msg}\n"
      end

      # The keys are the child PIDs, the values are information about the worker
      @children = {}

      # The interval for checking for new jobs
      @interval = options[:interval] || 5.0

      # The max interval between when children start (reduces thundering herds)
      @max_startup_delay = options[:max_startup_delay] || 10.0

      # The jobs that are getting processed, and the thread that's handling them
      @jids = {}
    end

    def job_reserver
      @reserver
    end

    # Set the proceline. Not supported on all systems
    def procline(value)
      $0 = "Qless-#{Qless::VERSION}: #{value} at #{Time.now.iso8601}"
      @log.info($0)
    end

    # Stop processing after this job
    def shutdown
      @shutdown = true
    end

    # Pause the worker -- take no more new jobs
    def pause
      @paused = true
      procline "Paused -- #{job_reserver.description}"
    end

    # Continue taking new jobs
    def unpause
      @paused = false
    end

    # The meaning of these signals is meant to closely mirror resque
    #
    # TERM: Shutdown immediately, stop processing jobs.
    #  INT: Shutdown immediately, stop processing jobs.
    # QUIT: Shutdown after the current job has finished processing.
    # USR1: Kill the forked children immediately, continue processing jobs.
    # USR2: Pause after this job
    # CONT: Start processing jobs again after a USR2
    def register_signal_handlers
      if @master
        # If we're the parent process, we mostly want to forward the signals on
        # to the child processes. It's just that sometimes we want to wait for
        # them and then exit
        trap('TERM') do
          stop!('TERM')
          exit
        end

        trap('INT') do
          stop!('TERM')
          exit
        end

        begin
          trap('QUIT') do
            stop!('QUIT')
            exit
          end
          trap('USR1') { stop!('KILL') }
          trap('USR2') { stop('USR2') }
          trap('CONT') { stop('CONT') }
        rescue ArgumentError
          warn "Signals QUIT, USR1, USR2, and/or CONT not supported."
        end
      else
        # Otherwise, we want to take the appropriate action
        trap('TERM') { exit! }
        trap('INT')  { exit! }
        begin
          trap('QUIT') { shutdown }
          trap('USR2') do
            pause
            @log.info("Current backtrace (child #{Process.pid}): \n\n" +
              "#{caller.join("\n")}\n\n")
          end
          trap('CONT') { unpause  }
        rescue ArgumentError
        end
      end
    end

    # Returns a list of each of the child pids
    def children
      @children.keys
    end

    # Signal all the children
    def stop(signal = 'QUIT')
      @log.warn("Sending #{signal} to children")
      children.each do |pid|
        Process::kill(signal, pid)
      end
    end

    # Signal all the children and wait for them to exit
    def stop!(signal = 'QUIT')
      # First, sent the signal
      stop(signal)

      # Wait for each of our children
      @log.warn('Waiting for child processes')
      until @children.empty?
        begin
          pid, _ = Process::wait2
          @log.warn("Child #{pid} stopped")
          @children.delete(pid)
        rescue SystemCallError
          break
        end
      end

      # If there were any children processes we couldn't wait for, log it
      @children.keys.each do |child_pid|
        @log.warn("Could not wait for child #{child_pid}")
      end
    end

    # Complete the job unless the worker has already put it into another state
    # by completing / failing / etc. the job
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
      @log.error("Failed to complete #{job.inspect}: #{e.message}")
    end

    # Actually perform the job
    def perform(job)
      begin
        around_perform(job)
      rescue JobLockLost => error
        @log.warn("Lost lock for job #{job.jid}")
      rescue Exception => error
        fail_job(job, error, caller)
      else
        try_complete(job)
      end
    end

    # Allow middleware modules to be mixed in and override the
    # definition of around_perform while providing a default
    # implementation so our code can assume the method is present.
    module SupportsMiddlewareModules
      def around_perform(job)
        job.perform
      end
    end

    include SupportsMiddlewareModules
  end

  class Worker < BaseWorker
    # Starts a worker based on ENV vars. Supported ENV vars:
    #   - REDIS_URL=redis://host:port/db-num (the redis gem uses this automatically)
    #   - QUEUES=high,medium,low or QUEUE=blah
    #   - JOB_RESERVER=Ordered or JOB_RESERVER=RoundRobin
    #   - INTERVAL=3.2
    #   - MAX_STARTUP_DELAY=2.4
    #   - VERBOSE=true (to enable logging)
    #   - VVERBOSE=true (to enable very verbose logging)
    def self.start
      # Split up the queues by comma
      client = Qless::Client.new
      queues = (ENV['QUEUES'] || ENV['QUEUE']).to_s.split(',').map do |q|
        client.queues[q.strip]
      end
      if queues.none?
        raise "You must pass QUEUE or QUEUES when starting a worker."
      end

      # Check for some of our deprecated env variables
      ['RUN_AS_SINGLE_PROCESS'].each do |deprecated|
        if ENV.has_key?(deprecated)
          puts "#{deprecated} is deprecated. Please refrain from using it"
        end
      end

      reserver = JobReservers.const_get(
        ENV.fetch('JOB_RESERVER', 'Ordered')).new(queues)

      options = {}
      options[:interval] = Float(ENV['INTERVAL']) if ENV['INTERVAL']
      if ENV['MAX_STARTUP_DELAY']
        options[:max_startup_delay] = Float(ENV['MAX_STARTUP_DELAY'])
      end
      options[:log_level] = Logger::WARN
      if !!ENV['VERBOSE']
        options[:log_level] = Logger::INFO
      elsif !!ENV['VVERBOSE']
        options[:log_level] = Logger::DEBUG
      end

      new(reserver, options).run
    end

    def initialize(reserver, options = {})
      super(reserver, options)
      # TODO: facter to figure out how many cores we have
      @num_workers = options[:num_workers] || 1
      # Whether or not this is the parent process
      @master = true
    end


    # Spawn children to do work, and run with it
    def run
      # Make sure we respond to signals correctly
      register_signal_handlers

      @log.debug("Starting to run with #{@num_workers} workers")
      @num_workers.times do |i|
        slot = {
          worker_id: i,
          sandbox: nil
        }
        child_pid = fork do
          # pause for a bit to calm the thundering herd
          sleep(Random.rand(max_startup_delay)) if max_startup_delay > 0

          # Otherwise, we'll do some work
          @master = false
          @sandbox = slot[:sandbox]
          @worker_id = slot[:worker_id]
          work
        end

        # If we're the parent process, save information about the child
        @log.info("Spawned worker #{child_pid}")
        @children[child_pid] = slot
      end

      # So long as I'm the parent process, I should keep an eye on my children
      while @master
        begin
          # Wait for any child to kick the bucket
          pid, status = Process::wait2
          code, sig = status.exitstatus, status.stopsig
          @log.warn(
            "Worker process #{pid} died with #{code} from signal (#{sig})")
          # And give its slot to a new worker process
          slot = @children.delete(pid)
          child_pid = fork do
            # Otherwise, we'll do some work
            @master = false
            @sandbox = slot[:sandbox]
            @worker_id = slot[:worker_id]
            # NOTE: In the case that the worker died, we're going to assume
            # that something about the job(s) it was working made the worker
            # exit, and so we're going to ignore any jobs that we might have
            # been working on. It's also significantly more difficult than the
            # above problem of simply distributing work to /new/ workers,
            # rather than a respawned worker.
            work
          end

          # If we're the parent process, ave information about the child
          @log.warn("Spawned worker #{child_pid} to replace #{pid}")
          @children[child_pid] = slot
        rescue SystemCallError
          @log.error('Failed to wait for child process')
          exit!
        end
      end
    end

    # Alias for work
    def start(interval = nil)
      work(interval)
    end

    # Keep popping jobs and doing work
    def work(interval = nil)
      @log.info("Starting #{job_reserver.description} in #{Process.pid}")
      procline "Starting #{job_reserver.description}"
      register_signal_handlers
      interval = interval || @interval

      # Reconnect each client
      uniq_clients.each { |client| client.redis.client.reconnect }

      job_reserver.prep_for_work!

      listen_for_lost_lock do
        loop do
          break if @shutdown
          if paused
            @log.debug('Paused')
            sleep interval
            next
          end

          unless job = reserve_job
            break if interval.zero?
            procline "Waiting for #{job_reserver.description}"
            @log.debug("Sleeping for #{interval} seconds")
            sleep interval
            next
          end

          begin
            @log.info("Starting job #{job.jid}")
            # Note that it's the main thread that's handling this job
            @jids[job.jid] = Thread.current
            perform(job)
            @log.debug("Finished job #{job.jid}")
          ensure
            # And remove the reference for this job
            @jids.delete(job.jid)
          end
        end
      end
    end

    # Get the next job from our reserver
    def reserve_job
      job_reserver.reserve
    rescue Exception => error
      # We want workers to durably stay up, so we don't want errors
      # during job reserving (e.g. network timeouts, etc) to kill
      # the worker.
      @log.error(
        "Got an error while reserving a job: #{error.class}: #{error.message}")
      return nil
    end

    def fail_job(job, error, worker_backtrace)
      failure = Qless.failure_formatter.format(job, error, worker_backtrace)
      job.fail(*failure)
      @log.error("Got #{failure.group} failure from #{job.inspect}")
    rescue Job::CantFailError => e
      # There's not much we can do here. Another worker may have cancelled it,
      # or we might not own the job, etc. Logging is the best we can do.
      @log.error("Failed to fail #{job.inspect}: #{e.message}")
    end

  private

    def deregister
      uniq_clients.each do |client|
        client.deregister_workers(Qless.worker_name)
      end
    end

    def uniq_clients
      @uniq_clients ||= job_reserver.queues.map(&:client).uniq
    end

    def listen_for_lost_lock
      subscribers = uniq_clients.map do |client|
        Subscriber.start(client, "ql:w:#{Qless.worker_name}") do |_, message|
          if message['event'] == 'lock_lost'
            thread = @jids[message['jid']]
            unless thread.nil?
              thread.raise(JobLockLost.new)
            end
          end
        end
      end

      yield
    ensure
      subscribers.each(&:stop)
    end
  end
end
