# Re-definitions are appended to existing tasks
task :environment
task :merb_env

namespace :jobs do
  desc "Clear the delayed_job queue."
  task :clear => :"clear:all"
  namespace :clear do

    desc "Clear the whole delayed_job queue."
    task :all => [:merb_env, :environment] do
      Delayed::Job.delete_all
    end
  end

  desc "Start a delayed_job worker."
  task :work => [:merb_env, :environment] do
    options={}
    [:min_priority, :max_priority, :max_attempts, :max_run_time, :sleep_delay].each do |key|
      value=ENV[key.to_s.upcase]
      options[key]=value if value.present?
    end
    Delayed::Worker.new(options).start
  end

  namespace :daemon do
    desc "Start a background delayed_job worker"
    task :start do
      `#{File.expand_path(File.dirname(__FILE__) + '/../../script/delayed_job')} start`
    end

    desc "Stop background delayed_job worker"
    task :stop do
      `#{File.expand_path(File.dirname(__FILE__) + '/../../script/delayed_job')} stop`
    end
  end
end
