require 'timeout'
require 'qless/middleware/requeue_exceptions'

module Qless
  # Unique error class used when a job is timed out by this middleware.
  # Allows us to differentiate this timeout from others caused by `::Timeout::Erorr`
  JobTimedoutError = Class.new(StandardError)
  InvalidTimeoutError = Class.new(ArgumentError)

  module Middleware
    # Applies a hard time out. To use this middleware, instantiate it and pass a block; the block
    # will be passed the job object (which has a `ttl` method for getting the job's remaining TTL),
    # and the block should return the desired timeout in seconds.
    # This allows you to set a hard constant time out to a particular job class
    # (using something like `extend Qless::Middleware::Timeout.new { 60 * 60 }`),
    # or a variable timeout based on the individual TTLs of each job
    # (using something like `extend Qless::Middleware::Timeout.new { |job| job.ttl * 1.1 }`).
    class Timeout < Module
      def initialize(opts = {})
        timeout_class = opts.fetch(:timeout_class, ::Timeout)
        kernel_class = opts.fetch(:kernel_class, Kernel)
        module_eval do
          define_method :around_perform do |job|
            timeout_seconds = yield job

            return super(job) if timeout_seconds.nil?

            if !timeout_seconds.is_a?(Numeric) || timeout_seconds <= 0
              raise InvalidTimeoutError, "Timeout must be a positive number or nil, " \
                                         "but was #{timeout_seconds}"
            end

            begin
              timeout_class.timeout(timeout_seconds) { super(job) }
            rescue ::Timeout::Error => e
              error = JobTimedoutError.new("Qless: job timeout (#{timeout_seconds}) exceeded.")
              error.set_backtrace(e.backtrace)
              # The stalled connection to redis might be the cause of the timeout. We cannot rely
              # on state of connection either (e.g., we might be in the middle of Redis call when
              # timeout happend). To play it safe, we reconnect.
              job.reconnect_to_redis
              job.fail(*Qless.failure_formatter.format(job, error, []))
              # Since we are leaving with bang (exit!), normal requeue logic does not work.
              # Do it manually right here.
              if self.is_a?(::Qless::Middleware::RequeueExceptions) &&
                 self.requeueable?(JobTimedoutError)
                self.handle_exception(job, error)
              end

              # ::Timeout.timeout is dangerous to use as it can leave things in an inconsistent
              # state. With Redis, for example, we've seen the socket buffer left with unread bytes
              # on it, which can affect later redis calls. Thus, it's much safer just to exit, and
              # allow the parent process to restart the worker in a known, clean state.
              #
              # We use 73 as a unique exit status for this case. 73 looks
              # a bit like TE (Timeout::Error)
              kernel_class.exit!(73)
            end
          end
        end
      end
    end
  end
end
