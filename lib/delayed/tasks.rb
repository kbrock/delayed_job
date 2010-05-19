# Re-definitions are appended to existing tasks
task :environment
task :merb_env

namespace :jobs do
  desc "Clear the delayed_job queue."
  task :clear => [:merb_env, :environment] do
    Delayed::Job.delete_all
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
end
