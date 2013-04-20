module Qless
  module WaitUntil
    TimeoutError = Class.new(StandardError)

    def wait_until(timeout)
      timeout_at = Time.now + timeout

      loop do
        return if yield
        sleep 0.002
        if Time.now > timeout_at
          raise TimeoutError, "Timed out after #{timeout} seconds"
        end
      end
    end

    module_function :wait_until
  end
end
