namespace :qless do
  task :setup # no-op; users should define their own setup

  desc "Start a Qless worker using env vars: QUEUES, JOB_RESERVER, REDIS_URL, INTERVAL, VERBOSE, VVERBOSE"
  task :work => :setup do
    require 'qless/worker'
    Qless::Worker.start
  end
end

