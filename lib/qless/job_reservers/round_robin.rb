# Encoding: utf-8

module Qless
  module JobReservers
    # Round-robins through all the provided queues
    class RoundRobin
      attr_reader :queues

      def initialize(queues)
        @queues = queues
        @num_queues = queues.size
        @last_popped_queue_index = @num_queues - 1
      end

      def reserve
        @num_queues.times do |i|
          job = next_queue.pop
          return job if job
        end
        nil
      end

      def prep_for_work!
        # nothing here on purpose
      end

      def description
        @description ||=
          @queues.map(&:name).join(', ') + " (#{self.class::TYPE_DESCRIPTION})"
      end

      def reset_description!
        @description = nil
      end

    private

      TYPE_DESCRIPTION = 'round robin'

      def next_queue
        @last_popped_queue_index = (@last_popped_queue_index + 1) % @num_queues
        @queues[@last_popped_queue_index]
      end
    end
  end
end
