module Delayed
  class Worker
    cattr_accessor :min_priority, :max_priority, :max_attempts, :max_run_time, :sleep_delay, :logger
    self.sleep_delay = 5
    self.max_attempts = 25
    self.max_run_time = 4.hours
    
    # By default failed jobs are destroyed after too many attempts. If you want to keep them around
    # (perhaps to inspect the reason for the failure), set this to false.
    cattr_accessor :destroy_failed_jobs
    self.destroy_failed_jobs = true

    # By default successful jobs are destroyed after finished.
    # If you want to keep them around (for statistics/monitoring),
    # set this to false.
    cattr_accessor :destroy_successful_jobs
    self.destroy_successful_jobs = true

    # only useful if destroy_successful_jobs == false
    # This will clear out the errors for a successful job
    # since it succeeded, no reason to keep around
    cattr_accessor :clear_successful_errors
    self.clear_successful_errors = false

    self.logger = if defined?(Merb::Logger)
      Merb.logger
    elsif defined?(Rails)
      Rails.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    # name_prefix is ignored if name is set directly
    attr_accessor :name_prefix

    def initialize(options={})
      @quiet = options.delete(:quiet)
      options.each_pair do |key,value|
        setter="#{key}="
        if self.class.respond_to? setter
          self.class.send(setter,value.to_i) if value.present?
        else
          say "unknown worker attribute #{key}"
        end
      end
    end

    # Every worker has a unique name which by default is the pid of the process. There are some
    # advantages to overriding this with something which survives worker retarts:  Workers can#
    # safely resume working on tasks which are locked by themselves. The worker will assume that
    # it crashed before.
    def name
      return @name unless @name.nil?
      "#{@name_prefix}host:#{Socket.gethostname} pid:#{Process.pid}" rescue "#{@name_prefix}pid:#{Process.pid}"
    end

    # Sets the name of the worker.
    # Setting the name to nil will reset the default worker name
    def name=(val)
      @name = val
    end

    def start
      say "*** Starting job worker #{name}"

      trap('TERM') { say 'Exiting...'; $exit = true }
      trap('INT')  { say 'Exiting...'; $exit = true }

      loop do
        result = nil

        realtime = Benchmark.realtime do
          result = work_off
        end

        count = result.sum

        break if $exit

        if count.zero?
          sleep(@@sleep_delay)
        else
          say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
        end

        break if $exit
      end

    ensure
      Delayed::Job.clear_locks!(name)
    end
    
    def run(job)
      runtime =  Benchmark.realtime do
        Timeout.timeout(self.class.max_run_time.to_i) { job.invoke_job }

        if destroy_successful_jobs
          job.destroy
        else
          new_attributes={:finished_at => Delayed::Job.db_time_now, :failed_at => nil, :locked_at => nil, :locked_by => nil}
          #sometimes, there is no reason to keep an error message (if the job ended up being successful)
          new_attributes[:last_error]=nil if clear_successful_errors
          job.update_attributes(new_attributes)
        end
      end
      # TODO: warn if runtime > max_run_time ?
      say "* [JOB] #{name} completed after %.4f" % runtime
      return true  # did work
    rescue Exception => e
      handle_failed_job(job, e)
      return false  # work failed
    rescue Delayed::DeserializationError => e
      handle_failed_job(job, e, true)
      return false  # work failed
    end

    # Reschedule the job in the future (when a job fails).
    # Uses an exponential scale depending on the number of failed attempts.
    def reschedule(job,error=nil,force_kill=false)
      # if we are not forcing a kill (default)
      # and the number of attempts is within the number of times we want to try
      if ! force_kill && job.attempts < self.class.max_attempts
        schedule(job,error)
      else
        remove(job,error)
      end
    end

    def schedule(job, error=nil)
      time = Job.db_time_now + (job.attempts ** 4) + 5
      job.run_at = time
      job.unlock
      job.save!
    end

    def remove(job, error=nil)
      say "* [JOB] PERMANENTLY removing #{job.name} because of #{job.attempts} consecutive failures.", Logger::INFO
      if self.class.destroy_failed_jobs
          job.destroy
      else
        job.update_attributes({:failed_at => Delayed::Job.db_time_now,:finished_at => nil, :locked_at => nil, :locked_by => nil})
      end
    end

    def say(text, level = Logger::INFO)
      puts text unless @quiet
      logger.add level, text if logger
    end

  protected
    
    #TODO: override this one
    def handle_failed_job(job, error, dont_run_again=false)
      job.last_error = error.message + "\n" + error.backtrace.join("\n")
      say "* [JOB] #{name} failed with #{error.class.name}: #{error.message} - #{job.attempts} failed attempts", Logger::ERROR
      reschedule(job,error,dont_run_again)
    end
    
    # Run the next job we can get an exclusive lock on.
    # If no jobs are left we return nil
    def reserve_and_run_one_job

      # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
      # this leads to a more even distribution of jobs across the worker processes
      job = Delayed::Job.find_available(name, 5, self.class.max_run_time).detect do |job|
        if job.lock_exclusively!(self.class.max_run_time, name)
          say "* [Worker(#{name})] acquired lock on #{job.name}"
          true
        else
          say "* [Worker(#{name})] failed to acquire exclusive lock for #{job.name}", Logger::WARN
          false
        end
      end

      run(job) if job
    end

    # Do num jobs and return stats on success/failure.
    # Exit early if interrupted.
    def work_off(num = 100)
      success, failure = 0, 0

      num.times do
        case reserve_and_run_one_job
        when true
            success += 1
        when false
            failure += 1
        else
          break  # leave if no work could be done
        end
        break if $exit # leave if we're exiting
      end

      return [success, failure]
    end
  end
end
