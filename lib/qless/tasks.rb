# Encoding: utf-8

namespace :qless do
  task :setup # no-op; users should define their own setup

  desc 'Start a worker with env: QUEUES, JOB_RESERVER, REDIS_URL, INTERVAL, WORKERS'
  task work: :setup do
    require 'qless/worker'
    qless = Qless::Client.new
    queues = ENV['QUEUES'].split(/\s*,\s*/).map { |name| qless.queues[name] }

    job_reserver = case ENV['JOB_RESERVER']
                   when 'RoundRobin'
                     require 'qless/job_reservers/round_robin'
                     Qless::JobReservers::RoundRobin.new(queues)
                   when 'ShuffledRoundRobin'
                     require 'qless/job_reservers/shuffeled_round_robin'
                     Qless::JobReservers::ShuffeledRoundRobin.new(queues)
                   else
                     #Ordered job reserver is the default
                     require 'qless/job_reservers/ordered'
                     Qless::JobReservers::Ordered.new(queues)
                   end
    log_level = Logger::DEBUG if ENV['VVERBOSE']
    log_level ||= Logger::INFO if ENV['VERBOSE']
    log_level ||= Logger::WARN
    Qless::Workers::ForkingWorker.new(job_reserver,
                                      :log_level   => log_level,
                                      :num_workers => (ENV['WORKERS'] || 1).to_i,
                                      :interval    => (ENV['INTERVAL'] || 2).to_i).run
  end
end
