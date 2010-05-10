require 'timeout'

module Delayed

  class DeserializationError < StandardError
  end

    # table.integer  :priority, :default => 0      # Allows some jobs to jump to the front of the queue
    # table.integer  :attempts, :default => 0      # Provides for retries, but still fail eventually.
    # table.text     :handler                      # YAML-encoded string of the object that will do work
    # table.text     :last_error                   # reason for last failure (See Note below)
    # table.datetime :run_at                       # When to run. Could be Time.zone.now for immediately, or sometime in the future.
    # table.datetime :locked_at                    # Set when a client is working on this object
    # table.datetime :failed_at                    # Set when all retries have failed (actually, by default, the record is deleted instead)
    # table.string   :locked_by                    # Who is working on this object (if locked)
    # table.datetime :first_started_at             # When first worker picked it up
    # table.datetime :last_started_at              # When last worker picked it up (same as first_started_at when no retries)
    # table.datetime :finished_at                  # Used for statiscics / monitoring
    # table.timestamps

  # A job object that is persisted to the database.
  # Contains the work object as a YAML field.
  class Job < ActiveRecord::Base

    #turn off cache money for this class (if cache money is installed)
    is_cached(false) if self.respond_to?(:is_cached)
    set_table_name :delayed_jobs

    named_scope :ready_to_run, lambda {|worker_name, max_run_time|
      {:conditions => ['(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR locked_by = ?) AND failed_at IS NULL and finished_at IS NULL', db_time_now, db_time_now - max_run_time, worker_name]}
    }
    named_scope :by_priority, :order => 'priority ASC, run_at ASC'
    named_scope :recent_first, :order => 'id DESC'
    named_scope :oldest_first, :order => 'id'

    named_scope :retry,    :conditions => 'locked_at IS NULL AND failed_at IS NULL AND finished_at IS NULL AND first_started_at IS NOT NULL'
    named_scope :finished, :conditions => 'finished_at IS NOT NULL'
    named_scope :failed,   :conditions => 'failed_at IS NOT NULL'
    #simplified ready clause for status pages
    named_scope :ready,    :conditions => 'locked_at IS NULL and failed_at IS NULL and finished_at IS NULL'
    #do we want a scope for outliers

    ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

    # When a worker is exiting, make sure we don't have any locked jobs.
    def self.clear_locks!(worker_name)
      update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
    end

    def failed?
      failed_at
    end
    alias_method :failed, :failed?

    def payload_object
      @payload_object ||= deserialize(self['handler'])
    end

    def deserializes?
      @deserializes||=
        begin
          payload_object
          true
        rescue DeserializationError
          false
        end
    end

    def name
      @name ||= if deserializes?
        payload = payload_object
        if payload.respond_to?(:display_name)
          payload.display_name
        else
          payload.class.name
        end
      else
        "Error"
      end
    end

    #would be better if we could get all params for the object and do a hash
    def params
      if deserializes?
        payload=payload_object
        payload.respond_to?(:to_h) ? payload.to_h : nil
      else
        nil
      end
    end

    def payload_object=(object)
      self['handler'] = object.to_yaml
    end

    # Add a job to the queue
    def self.enqueue(*args, &block)
      object = block_given? ? EvaledJob.new(&block) : args.shift

      unless object.respond_to?(:perform) || block_given?
        raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
      end
    
      priority = args.first || 0
      run_at   = args[1]

      Job.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
    end

    # Find a few candidate jobs to run (in case some immediately get locked by others).
    def self.find_available(worker_name, limit = 5, max_run_time = Worker.max_run_time)
      scope = self.ready_to_run(worker_name, max_run_time)
      scope = scope.scoped(:conditions => ['priority >= ?', Worker.min_priority]) if Worker.min_priority
      scope = scope.scoped(:conditions => ['priority <= ?', Worker.max_priority]) if Worker.max_priority
      
      ActiveRecord::Base.silence do
        scope.by_priority.all(:limit => limit)
      end
    end

    # Lock this job for this worker.
    # Returns true if we have the lock, false otherwise.
    def lock_exclusively!(max_run_time, worker)
      now = self.class.db_time_now
      
      #whether this job has run before in the past
      first_time=self.first_started_at.nil?

      #attributes to modify in the job table
      conditions="locked_at = ?, last_started_at = ?"
      attrs=[now,now]

      #if it hasn't been run, then we want to also update first_started_at
      if first_time
        conditions+=", first_started_at = ?"
        attrs << now
      end

      if locked_by != worker
        # We don't own this job so we will also update the locked_by name
        conditions+=", locked_by = ?"
        attrs.unshift(conditions)
        attrs << worker
        affected_rows = self.class.update_all(attrs,
          ["id = ? and (locked_at is null or locked_at < ?) and (run_at <= ?)", id, (now - max_run_time.to_i), now])
      else
        # We already own this job, this may happen if the job queue crashes.
        # Simply resume and update the locked_at
        attrs.unshift(conditions)
        affected_rows = self.class.update_all(attrs, ["id = ? and locked_by = ?", id, worker])
      end

      if affected_rows == 1
        #update the attributes to the same values that were set in the database
        self.locked_at          = now
        self.last_started_at    = now
        self.first_started_at ||= now
        self.locked_by          = worker
        return true
      else
        return false
      end
    end

    # Unlock this job (note: not saved to DB)
    def unlock
      self.locked_at    = nil
      self.locked_by    = nil
    end

    # Moved into its own method so that new_relic can trace it.
    def invoke_job
      self.attempts += 1
      payload_object.perform
    end

  private

    def deserialize(source)
      handler = YAML.load(source) rescue nil

      unless handler.respond_to?(:perform)
        #yaml in 1.8.5 does not namespace the handler.class - so bypass for now
        #used to use handler.class if ! handler.nil?
        if source =~ ParseObjectFromYaml
          handler_class = $1
        end
        attempt_to_load(handler_class || handler.class.to_s)
        handler = YAML.load(source)
      end

      return handler if handler.respond_to?(:perform)

      raise DeserializationError,
        'Job failed to load: Unknown handler. Try to manually require the appropriate file.'
    rescue TypeError, LoadError, NameError => e
      raise DeserializationError,
        "Job failed to load: #{e.message}. Try to manually require the required file."
    end

    # Constantize the object so that ActiveSupport can attempt
    # its auto loading magic. Will raise LoadError if not successful.
    def attempt_to_load(klass)
       klass.constantize
    end

    # Get the current time (GMT or local depending on DB)
    # Note: This does not ping the DB to get the time, so all your clients
    # must have syncronized clocks.
    def self.db_time_now
      if Time.zone
        Time.zone.now
      elsif ActiveRecord::Base.default_timezone == :utc
        Time.now.utc
      else
        Time.now
      end
    end

  protected

    def before_save
      self.run_at ||= self.class.db_time_now
    end

  end

  class EvaledJob
    def initialize
      @job = yield
    end

    def perform
      eval(@job)
    end
  end
end
