# Encoding: utf-8

namespace :qless do
  task :setup # no-op; users should define their own setup

  desc 'Start a worker: QUEUES, JOB_RESERVER, REDIS_URL, INTERVAL, NUM_WORKERS'
  task work: :setup do
    # This creates a simple forking worker using the round-robin reserver
    require 'qless'
    require 'qless/workers/forking'
    require 'qless/job_reservers/round_robin'

    # This honors REDIS_URL
    client = Qless::Client.new

    # Read 'QUEUES', like 'foo,bar,whiz' and create the reserver
    queues = ENV.fetch('QUEUES', '').split(',').map { |q| client.queues[q] }
    reserver = Qless::JobReservers::RoundRobin.new(queues)
    
    # And start the worker with INTERVAL and NUM_WORKERS
    workers = ENV.fetch('NUM_WORKERS', 8).to_i
    interval = ENV.fetch('INTERVAL', 60).to_i
    Qless::Workers::ForkingWorker.new(reserver,
                                      interval: interval,
                                      num_workers: workers).run
  end
end
