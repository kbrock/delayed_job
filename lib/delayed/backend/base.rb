module Delayed
  module Backend
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

    module Base
      def self.included(base)
        base.extend ClassMethods
      end

      module ClassMethods
        # Add a job to the queue
        def enqueue(*args)
          object = args.shift
          unless object.respond_to?(:perform)
            raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
          end

          priority = args.first || 0
          run_at   = args[1]
          self.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
        end

        # Hook method that is called before a new worker is forked
        def before_fork
        end
        
        # Hook method that is called after a new worker is forked
        def after_fork
        end

        def work_off(num = 100)
          warn "[DEPRECATION] `Delayed::Job.work_off` is deprecated. Use `Delayed::Worker.new.work_off instead."
          Delayed::Worker.new.work_off(num)
        end
      end #ClassMethods

      ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

      def failed?
        failed_at
      end
      alias_method :failed, :failed?

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
        self.handler = object.to_yaml
      end
      
      def payload_object
        @payload_object ||= YAML.load(self.handler)
      rescue TypeError, LoadError, NameError => e
          raise DeserializationError,
            "Job failed to load: #{e.message}. Try to manually require the required file. Handler: #{handler.inspect}"
      end

      # YAML has been giving us troubles
      # so making it not puke when there is a serialization error
      def deserializes?
        @deserializes||=begin
          payload_object
          true
        rescue DeserializationError
          false
        end
      end

      # Moved into its own method so that new_relic can trace it.
      def invoke_job
        self.attempts += 1
        payload_object.perform
      end
      
      # Unlock this job (note: not saved to DB)
      def unlock
        self.locked_at    = nil
        self.locked_by    = nil
      end
      
    protected

      def set_default_run_at
        self.run_at ||= self.class.db_time_now
      end

    end
  end
end