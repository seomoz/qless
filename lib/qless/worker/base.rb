# Encoding: utf-8

# Standard stuff
require 'time'
require 'logger'

# Qless requires
require 'qless'
require 'qless/subscriber'

module Qless
  module Workers
    JobLockLost = Class.new(StandardError)

    class BaseWorker
      # An IO-like object that logging output is sent to.
      # Defaults to $stdout.
      attr_accessor :output, :reserver, :log_level, :interval, :paused, :log,
                    :jids, :options

      def initialize(reserver, options = {})
        # Our job reserver and options
        @reserver = reserver
        @options = options

        # Our logger
        @log = Logger.new(options[:output] || $stdout)
        @log_level = options[:log_level] || Logger::WARN
        @log.level = @log_level
        @log.formatter = proc do |severity, datetime, progname, msg|
          "#{datetime}: #{msg}\n"
        end

        # The interval for checking for new jobs
        @interval = options[:interval] || 5.0

        # The jobs that are getting processed, and the handling thread
        @jids = {}
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
        # Otherwise, we want to take the appropriate action
        trap('TERM') { exit! }
        trap('INT')  { exit! }
        begin
          trap('QUIT') { shutdown }
          trap('USR2') { pause    }
          trap('CONT') { unpause  }
        rescue ArgumentError
          warn 'Signals QUIT, USR1, USR2, and/or CONT not supported.'
        end
      end

      # Return an enumerator to each of the jobs provided by the reserver
      def jobs
        return Enumerator.new do |enum|
          loop do
            begin
              job = reserver.reserve
            rescue Exception => error
              # We want workers to durably stay up, so we don't want errors
              # during job reserving (e.g. network timeouts, etc) to kill the
              # worker.
              log.error(
                "Error reserving job: #{error.class}: #{error.message}")
            end

            # If we ended up getting a job, yield it. Otherwise, we wait
            if job.nil?
              no_job_available
            else
              enum.yield(job)
            end

            break if @shutdown
          end
        end
      end

      # Actually perform the job
      def perform(job)
        around_perform(job)
      rescue JobLockLost
        log.warn("Lost lock for job #{job.jid}")
      rescue Exception => error
        fail_job(job, error, caller)
      else
        try_complete(job)
      end

      # Allow middleware modules to be mixed in and override the
      # definition of around_perform while providing a default
      # implementation so our code can assume the method is present.
      module SupportsMiddlewareModules
        def around_perform(job)
          job.perform
        end

        def after_fork
        end
      end

      include SupportsMiddlewareModules

      # Stop processing after this job
      def shutdown
        @shutdown = true
      end

      # Pause the worker -- take no more new jobs
      def pause
        @paused = true
        procline "Paused -- #{reserver.description}"
      end

      # Continue taking new jobs
      def unpause
        @paused = false
      end

      # Set the proceline. Not supported on all systems
      def procline(value)
        $0 = "Qless-#{Qless::VERSION}: #{value} at #{Time.now.iso8601}"
        log.info($PROGRAM_NAME)
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
        log.error("Failed to complete #{job.inspect}: #{e.message}")
      end

      def fail_job(job, error, worker_backtrace)
        failure = Qless.failure_formatter.format(job, error, worker_backtrace)
        job.fail(*failure)
        log.error("Got #{failure.group} failure from #{job.inspect}")
      rescue Job::CantFailError => e
        # There's not much we can do here. Another worker may have cancelled it,
        # or we might not own the job, etc. Logging is the best we can do.
        log.error("Failed to fail #{job.inspect}: #{e.message}")
      end

      def deregister
        uniq_clients.each do |client|
          client.deregister_workers(Qless.worker_name)
        end
      end

      def uniq_clients
        @uniq_clients ||= reserver.queues.map(&:client).uniq
      end

      def listen_for_lost_lock
        subscribers = uniq_clients.map do |client|
          Subscriber.start(client, "ql:w:#{Qless.worker_name}") do |_, message|
            if message['event'] == 'lock_lost'
              thread = @jids[message['jid']]
              thread.raise(JobLockLost.new) unless thread.nil?
            end
          end
        end

        yield
      ensure
        subscribers.each(&:stop)
      end

    private

      def no_job_available
        unless interval.zero?
          procline "Waiting for #{reserver.description}"
          log.debug("Sleeping for #{interval} seconds")
          sleep interval
        end
      end
    end
  end
end
