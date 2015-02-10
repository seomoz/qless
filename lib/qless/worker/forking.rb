# Encoding: utf-8

# Qless requires
require 'qless'
require 'qless/worker/base'
require 'qless/worker/serial'
require 'thread'

module Qless
  module Workers
    class ForkingWorker < BaseWorker
      # The child startup interval
      attr_accessor :max_startup_interval

      def initialize(reserver, options = {})
        super(reserver, options)
        # The keys are the child PIDs, the values are information about the
        # worker, including its sandbox directory. This directory currently
        # isn't used, but this sets up for having that eventually.
        @sandboxes = {}

        # Save our options for starting children
        @options = options

        # The max interval between when children start (reduces thundering herd)
        @max_startup_interval = options[:max_startup_interval] || 10.0

        # TODO: facter to figure out how many cores we have
        @num_workers = options[:num_workers] || 1

        # All the modules that have been applied to this worker
        @modules = []

        @sandbox_mutex = Mutex.new
        # A list of [signal_name, exit_parent]
        @signal_queue = []
      end

      # Because we spawn a new worker, we need to apply all the modules that
      # extend this one
      def extend(mod)
        @modules << mod
        super(mod)
      end

      # Spawn a new child worker
      def spawn
        worker = SerialWorker.new(reserver, @options)
        # We use 11 as the exit status so that it is something unique
        # (rather than the common 1). Plus, 11 looks a little like
        # ll (i.e. "Lock Lost").
        worker.on_current_job_lock_lost { |job| exit!(11) }
        @modules.each { |mod| worker.extend(mod) }
        worker
      end

      # If @sandbox_mutex is free, handles a signal now, shutting down workers
      # and possibly the main thread. If @sandbox_mutex is locked, adds the signal
      # to the signal queue, to be handled as soon as it can (see
      # process_signal_queue as called by the main loop)
      def handle_signal(signal_name, exit_parent)
        if @sandbox_mutex.try_lock
          stop!(signal_name, exit_parent)
          @sandbox_mutex.unlock
        else
          @signal_queue << [signal_name, exit_parent]
        end
      end

      # Process any signals (such as TERM) that could not be processed
      # immediately due to @sandbox_mutex being in use
      def process_signal_queue
        until @signal_queue.empty?
          signal_name, exit_parent = @signal_queue.shift
          stop!(signal_name, exit_parent)
        end
      end

      # Register our handling of signals
      def register_signal_handlers
        # If we're the parent process, we mostly want to forward the signals on
        # to the child processes. It's just that sometimes we want to wait for
        # them and then exit
        trap('TERM') do
          handle_signal('TERM', true)
        end

        trap('INT') do
          handle_signal('TERM', true)
        end

        begin
          trap('QUIT') do
            handle_signal('QUIT', true)
          end
          trap('USR1') { handle_signal('KILL', false) }
          trap('USR2') { stop('USR2') }
          trap('CONT') { stop('CONT') }
        rescue ArgumentError
          warn 'Signals QUIT, USR1, USR2, and/or CONT not supported.'
        end
      end

      # Run this worker
      def run
        startup_sandboxes

        # Now keep an eye on our child processes, spawn replacements as needed
        loop do
          begin
            process_signal_queue

            # Don't wait on any processes if we're already in shutdown mode.
            break if @shutdown

            # Wait for any child to kick the bucket
            pid, status = Process.wait2
            code, sig = status.exitstatus, status.stopsig
            log(:warn,
              "Worker process #{pid} died with #{code} from signal (#{sig})")

            # allow our shutdown logic (called from a separate thread) to take affect.
            break if @shutdown

            spawn_replacement_child(pid)
          rescue SystemCallError => e
            log(:error, "Failed to wait for child process: #{e.inspect}")
            # If we're shutting down, the loop above will exit
            exit! unless @shutdown
          end
        end
      end

      # Returns a list of each of the child pids
      def children
        @sandboxes.keys
      end

      # Signal all the children
      def stop(signal = 'QUIT')
        log(:warn, "Sending #{signal} to children")
        children.each do |pid|
          begin
            Process.kill(signal, pid)
          rescue Errno::ESRCH
            # no such process -- means the process has already died.
          end
        end
      end

      # Signal all the children and wait for them to exit. Optionally have this process (parent) die, too.
      # Should only be called when we have the lock on @sandbox_mutex
      def stop!(signal = 'QUIT', exit_parent = false)
        shutdown
        shutdown_sandboxes(signal)
        exit  if exit_parent
      end

    private

      def startup_sandboxes
        # Make sure we respond to signals correctly
        register_signal_handlers

        log(:debug, "Starting to run with #{@num_workers} workers")
        @num_workers.times do |i|
          slot = {
            worker_id: i,
            sandbox: nil
          }

          cpid = fork_child_process do
            # Wait for a bit to calm the thundering herd
            sleep(rand(max_startup_interval)) if max_startup_interval > 0
          end

          # If we're the parent process, save information about the child
          log(:info, "Spawned worker #{cpid}")
          @sandboxes[cpid] = slot
        end
      end

      # Should only be called when we have a lock on @sandbox_mutex
      def shutdown_sandboxes(signal)
        # First, send the signal
        stop(signal)

        # Wait for each of our children
        log(:warn, 'Waiting for child processes')

        until @sandboxes.empty?
          begin
            pid, _ = Process.wait2
            log(:warn, "Child #{pid} stopped")
            @sandboxes.delete(pid)
          rescue SystemCallError
            break
          end
        end

        log(:warn, 'All children have stopped')

        # If there were any children processes we couldn't wait for, log it
        @sandboxes.keys.each do |cpid|
          log(:warn, "Could not wait for child #{cpid}")
        end

        @sandboxes.clear
      end

    private

      def spawn_replacement_child(pid)
        @sandbox_mutex.synchronize do
          return if @shutdown

          # And give its slot to a new worker process
          slot = @sandboxes.delete(pid)
          cpid = fork_child_process

          # If we're the parent process, ave information about the child
          log(:warn, "Spawned worker #{cpid} to replace #{pid}")
          @sandboxes[cpid] = slot
        end
      end

      # returns child's pid.
      def fork_child_process
        fork do
          yield if block_given?
          reconnect_each_client
          after_fork
          spawn.run
        end
      end

    end
  end
end
