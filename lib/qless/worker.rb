require 'qless'
require 'time'

module Qless
  # This is heavily inspired by Resque's excellent worker:
  # https://github.com/defunkt/resque/blob/v1.20.0/lib/resque/worker.rb
  class Worker
    def initialize(client, job_reserver)
      @client, @job_reserver = client, job_reserver
      @shutdown = @paused = false
    end

    def work(interval = 5.0)
      procline "Starting #{@job_reserver.description}"
      register_signal_handlers

      loop do
        break if shutdown?
        next  if paused?

        unless job = @job_reserver.reserve
          break if interval.zero?
          procline "Waiting for #{@job_reserver.description}"
          sleep interval
          next
        end

        if @child = fork
          # We're in the parent process
          procline "Forked #{@child} for #{job.description}"
          Process.wait(@child)
        else
          # We're in the child process
          procline "Processing #{job.description}"
          perform(job)
          exit!
        end
      end
    end

    def perform(job)
      job.perform
    rescue => error
      fail_job(job, error)
    else
      job.complete unless job.state_changed?
    end

    def shutdown
      @shutdown = true
    end

    def shutdown!
      shutdown
      kill_child
    end

    def shutdown?
      @shutdown
    end

    def paused?
      @paused
    end

    def pause_processing
      @paused = true
      procline "Paused -- #{@job_reserver.description}"
    end

    def unpause_processing
      @paused = false
    end

  private

    def fail_job(job, error)
      group = "#{job.klass}:#{error.class}"
      message = "#{error.message}\n\n#{error.backtrace.join("\n")}"
      job.fail(group, message)
    end

    def handle_child_from_parent(child)
      Process.wait(child)
    end

    def procline(value)
      $0 = "Qless: #{value} at #{Time.now.iso8601}"
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
  end
end

