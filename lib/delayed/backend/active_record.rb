require 'active_record'

class ActiveRecord::Base
  yaml_as "tag:ruby.yaml.org,2002:ActiveRecord"

  def self.yaml_new(klass, tag, val)
    klass.find(val['attributes']['id'])
  rescue ActiveRecord::RecordNotFound
    nil
  end

  def to_yaml_properties
    ['@attributes']
  end
end

module Delayed
  module Backend
    module ActiveRecord
      # A job object that is persisted to the database.
      # Contains the work object as a YAML field.
      class Job < ::ActiveRecord::Base
        include Delayed::Backend::Base
        set_table_name :delayed_jobs

        before_save :set_default_run_at

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


        def self.after_fork
          ::ActiveRecord::Base.connection.reconnect!
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
        end

        def failed?
          failed_at
        end
        alias_method :failed, :failed?

        # Find a few candidate jobs to run (in case some immediately get locked by others).
        def self.find_available(worker_name, limit = 5, max_run_time = Worker.max_run_time)
          scope = self.ready_to_run(worker_name, max_run_time)
          scope = scope.scoped(:conditions => ['priority >= ?', Worker.min_priority]) if Worker.min_priority
          scope = scope.scoped(:conditions => ['priority <= ?', Worker.max_priority]) if Worker.max_priority
      
          ::ActiveRecord::Base.silence do
            scope.by_priority.all(:limit => limit)
          end
        end

        # Lock this job for this worker.
        # Returns true if we have the lock, false otherwise.
        def lock_exclusively!(max_run_time, worker)
          now = self.class.db_time_now

          #TODO: just do an array of conditions
          #attributes to modify in the job table
          conditions="locked_at = ?, last_started_at = ?"
          attrs=[now,now]

          #if it hasn't been run, then we want to also update first_started_at
          if self.first_started_at.nil?
            conditions+=", first_started_at = ?"
            attrs << now
          end

          affected_rows = if locked_by != worker
            # We don't own this job so we will also update the locked_by
            conditions+=", locked_by = ?"
            attrs.unshift(conditions)
            attrs << worker

            self.class.update_all(attrs,
              ["id = ? and (locked_at is null or locked_at < ?) and (run_at <= ?)", id, (now - max_run_time.to_i), now])
          else
            # We already own this job, this may happen if the job queue crashes.
            # Simply resume and update the locked_at
            attrs.unshift(conditions)
            self.class.update_all(attrs, ["id = ? and locked_by = ?", id, worker])
          end

          if affected_rows == 1
            #update the attributes to the same values that were set in the database
            self.locked_at          = now
            self.locked_by          = worker
            self.last_started_at    = now
            self.first_started_at ||= now
            return true
          else
            return false
          end
        end

        # Get the current time (GMT or local depending on DB)
        # Note: This does not ping the DB to get the time, so all your clients
        # must have syncronized clocks.
        def self.db_time_now
          if Time.zone
            Time.zone.now
          elsif ::ActiveRecord::Base.default_timezone == :utc
            Time.now.utc
          else
            Time.now
          end
        end

      end
    end
  end
end
