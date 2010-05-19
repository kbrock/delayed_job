require 'timeout'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/class/attribute_accessors'
require 'active_support/core_ext/kernel'

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
    
    self.logger = if defined?(Merb::Logger)
      Merb.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    # name_prefix is ignored if name is set directly
    attr_accessor :name_prefix
    
    cattr_reader :backend
    
    def self.backend=(backend)
      if backend.is_a? Symbol
        require "delayed/backend/#{backend}"
        backend = "Delayed::Backend::#{backend.to_s.classify}::Job".constantize
      end
      @@backend = backend
      silence_warnings { ::Delayed.const_set(:Job, backend) }
    end
    
    def self.guess_backend
      self.backend ||= if defined?(ActiveRecord)
        :active_record
      elsif defined?(MongoMapper)
        :mongo_mapper
      else
        logger.warn "Could not decide on a backend, defaulting to active_record"
        :active_record
      end
    end

    # may want to explicitly define these
    # worker class:
    #   quiet, name, name_prefix, max_run_time, sleep_delay, max_attempts
    # class attributes with integer conversion:
    #   min_priority, max_priority (converted to integer)
    # class attributes (no conversion - accepts string or not coming from command line)
    #   destroy_failed_jobs, destroy_successful_jobs, clear_successful_errors
    def initialize(options={})
      options.each_pair do |key,value|
        if value.present?
          value=value.to_i if [:max_priority, :min_priority, :max_attempts, :sleep_delay].include?(key)
          setter="#{key}="
          if self.respond_to? setter
            self.send(setter,value)
          elsif self.class.respond_to? setter
            self.class.send(setter,value)
          else
            say "unknown worker attribute #{key}"
          end
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
      say "Starting job worker #{name}"

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
    
    def run(job)
      runtime =  Benchmark.realtime do
        Timeout.timeout(self.class.max_run_time.to_i) { job.invoke_job }
        job.destroy
      end
      say "#{job.name} completed after %.4f" % runtime
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
      say "PERMANENTLY removing #{job.name} because of #{job.attempts} consecutive failures.", Logger::INFO
      if job.payload_object.respond_to? :on_permanent_failure
        say "Running on_permanent_failure hook"
        job.payload_object.on_permanent_failure
      end
      if self.class.destroy_failed_jobs
        job.destroy
      else
        job.update_attributes({:failed_at => Delayed::Job.db_time_now, :finished_at => nil, :locked_at => nil, :locked_by => nil})
      end
    end

    def say(text, level = Logger::INFO)
      text = "[Worker(#{name})] #{text}"
      puts text unless @quiet
      logger.add level, "#{Time.now.strftime('%FT%T%z')}: #{text}" if logger
    end

  protected
    
    #TODO: override this one
    def handle_failed_job(job, error, dont_run_again=false)
      job.last_error = error.message + "\n" + error.backtrace.join("\n")
      say "#{job.name} failed with #{error.class.name}: #{error.message} - #{job.attempts} failed attempts", Logger::ERROR
      reschedule(job, error, dont_run_again)
    end
    
    # Run the next job we can get an exclusive lock on.
    # If no jobs are left we return nil
    def reserve_and_run_one_job

      # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
      # this leads to a more even distribution of jobs across the worker processes
      job = Delayed::Job.find_available(name, 5, self.class.max_run_time).detect do |job|
        if job.lock_exclusively!(self.class.max_run_time, name)
          say "acquired lock on #{job.name}"
          true
        else
          say "failed to acquire exclusive lock for #{job.name}", Logger::WARN
          false
        end
      end

      run(job) if job
    end
  end
end
