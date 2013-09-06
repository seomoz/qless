module Qless
  class FailureFormatter
    Failure = Struct.new(:group, :message) do
      # allow de-structring assignment
      def to_ary
        [group, message]
      end
    end

    def initialize
      @backtrace_replacements = { Dir.pwd => '.' }
      @backtrace_replacements[ENV['GEM_HOME']] = '<GEM_HOME>' if ENV.has_key?('GEM_HOME')
    end

    def format(job, error, lines_to_remove = caller(2))
      group = "#{job.klass_name}:#{error.class}"
      message = "#{truncated_message(error)}\n\n#{format_failure_backtrace(error.backtrace, lines_to_remove)}"

      Failure.new(group, message)
    end

  private

    # TODO: pull this out into a config option.
    MAX_ERROR_MESSAGE_SIZE = 10_000
    def truncated_message(error)
      return error.message if error.message.length <= MAX_ERROR_MESSAGE_SIZE
      error.message.slice(0, MAX_ERROR_MESSAGE_SIZE) + "... (truncated due to length)"
    end

    def format_failure_backtrace(error_backtrace, lines_to_remove)
      (error_backtrace - lines_to_remove).map do |line|
        @backtrace_replacements.inject(line) do |formatted, (original, new)|
          formatted.sub(original, new)
        end
      end.join("\n")
    end
  end
end

