# Encoding: utf-8

namespace :qless do
  task :setup # no-op; users should define their own setup

  desc 'Start a worker with env: QUEUES, JOB_RESERVER, REDIS_URL, INTERVAL'
  task work: :setup do
    require 'qless/worker'
    Qless::Worker.start
  end
end
