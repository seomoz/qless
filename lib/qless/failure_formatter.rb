# Encoding: utf-8

module Qless
  # A helper for formatting failure messages
  class FailureFormatter
    Failure = Struct.new(:group, :message) do
      # allow de-structring assignment
      def to_ary
        [group, message]
      end
    end

    def initialize
      @replacements = { Dir.pwd => '.' }
      @replacements[ENV['GEM_HOME']] = '<GEM_HOME>' if ENV.key?('GEM_HOME')
    end

    # lib/qless/job.rb#fail shows us that qless, right down to the Lua scripts,
    # is set up to expect both a group and a message for a failed job. So we
    # can't stop storing failed jobs altogether. But, to save on precious RAM,
    # we can stop recording the message, which is the stack traces that we currently
    # store.
    def format(job, error, lines_to_remove = caller(2))
      group = "#{job.klass_name}:#{error.class}"
      message = "#{truncated_message(error)}"
      Failure.new(group, message)
    end

  private

    # TODO: pull this out into a config option.
    MAX_ERROR_MESSAGE_SIZE = 100
    def truncated_message(error)
      return error.message if error.message.length <= MAX_ERROR_MESSAGE_SIZE
      error.message.slice(0, MAX_ERROR_MESSAGE_SIZE) +
        "\n... (truncated due to length)"
    end
  end
end
