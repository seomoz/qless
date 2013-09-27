# Encoding: utf-8

module Qless
  module JobReservers
    class Ordered
      attr_reader :queues

      def initialize(queues)
        @queues = queues
      end

      def reserve
        @queues.each do |q|
          job = q.pop
          return job if job
        end
        nil
      end

      def prep_for_work!
        # nothing here on purpose
      end

      def description
        @description ||= @queues.map(&:name).join(', ') + ' (ordered)'
      end
    end
  end
end
