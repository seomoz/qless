module QlessWebDevHelper
  class NoPerformJob; end;
  class << self
    def create_mock_jobs(client)
      queues = []

      num_queues = 5
      num_queues.times { |i|
        random_queue_name = 'foo-' + random_jid
        queues << client.queues[random_queue_name]
      }

      num_jobs = 100
      num_jobs.times {
        queues.sample.put('Qless::Job', {}, jid: random_jid)
      }
      num_jobs.times {
        queue = queues.sample
        queue.put(NoPerformJob, {}, jid: random_jid)
        queue.pop.perform
      }
      # successfully complete 10% of jobs
      (num_jobs*0.1).to_i.times {
        job = queues.sample.pop
        job.perform if job
      }
    end

    private
    def random_jid
      ('a'..'z').to_a.shuffle[0,8].join
    end
  end
end
