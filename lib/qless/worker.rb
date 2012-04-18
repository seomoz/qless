require 'qless'

module Qless
  class Worker
    def initialize(client, job_reserver)
      @client, @job_reserver = client, job_reserver
    end

    def work(interval = 5.0)
      loop do
        unless job = @job_reserver.reserve
          break if interval.zero?
          sleep interval
          next
        end

        if child = fork
          # We're in the parent process
          Process.wait(child)
        else
          # We're in the child process
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
  end
end

