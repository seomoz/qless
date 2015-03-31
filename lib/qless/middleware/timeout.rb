require 'timeout'

module Qless
  # Unique error class used when a job is timed out by this middleware.
  # Allows us to differentiate this timeout from others caused by `::Timeout::Erorr`
  JobTimedoutError = Class.new(StandardError)

  module Middleware
    # Applies a hard time out. To use this middleware, instantiate it and pass a block; the block
    # will be passed the job object (which has a `ttl` method for getting the job's remaining TTL),
    # and the block should return the desired timeout in seconds.
    # This allows you to set a hard constant time out to a particular job class
    # (using something like `extend Qless::Middleware::Timeout.new { 60 * 60 }`),
    # or a variable timeout based on the individual TTLs of each job
    # (using something like `extend Qless::Middleware::Timeout.new { |job| job.ttl * 1.1 }`).
    class Timeout < Module
      def initialize
        module_eval do
          define_method :around_perform do |job|
            timeout_value = yield job

            begin
              ::Timeout.timeout(timeout_value) { super(job) }
            rescue ::Timeout::Error => e
              error = JobTimedoutError.new(e.message)
              error.set_backtrace(e.backtrace)
              job.fail(*Qless.failure_formatter.format(job, error, []))

              # ::Timeout.timeout is dangerous to use as it can leave things in an inconsistent state.
              # With Redis, for example, we've seen the socket buffer left with unread bytes on it,
              # which can affect later redis calls.
              # Thus, it's much safer just to exit, and allow the parent process to restart the
              # worker in a known, clean state.
              # We use 73 as a unique exit status for this case. 73 looks
              # a bit like TE (Timeout::Error)
              exit!(73)
            end
          end
        end
      end
    end
  end
end
