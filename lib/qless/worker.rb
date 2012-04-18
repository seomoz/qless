require 'qless'
require 'time'

module Qless
  class Worker
    def initialize(client, job_reserver)
      @client, @job_reserver = client, job_reserver
    end

    def work(interval = 5.0)
      procline "Starting #{@job_reserver.description}"

      loop do
        unless job = @job_reserver.reserve
          break if interval.zero?
          procline "Waiting for #{@job_reserver.description}"
          sleep interval
          next
        end

        if child = fork
          # We're in the parent process
          procline "Forked #{child} for #{job.description}"
          Process.wait(child)
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
      job.complete
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
  end
end

