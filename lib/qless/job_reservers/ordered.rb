module Qless
  module JobReservers
    class Ordered
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

      def description
        @description ||= @queues.map(&:name).join(', ') + " (ordered)"
      end
    end
  end
end
