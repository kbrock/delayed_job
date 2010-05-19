require 'spec_helper'

describe Delayed::Worker do
  def job_create(opts = {})
    Delayed::Job.create({:payload_object => SimpleJob.new}.merge(opts))
  end

  def worker_create(opts ={})
    Delayed::Worker.new(
      {:max_priority => nil, :min_priority => nil, :quiet => true,
      :destroy_successful_jobs => true, :destroy_failed_jobs => true,
      :max_run_time => 10.second
      }.merge(opts)
    )
  end

  describe "backend=" do
    before do
      @clazz = Class.new
      Delayed::Worker.backend = @clazz
    end

    it "should set the Delayed::Job constant to the backend" do
      Delayed::Job.should == @clazz
    end

    it "should set backend with a symbol" do
      Delayed::Worker.backend = :active_record
      Delayed::Worker.backend.should == Delayed::Backend::ActiveRecord::Job
    end
  end

  BACKENDS.each do |backend|
    describe "with the #{backend} backend" do
      before do
        Delayed::Worker.backend = backend
        Delayed::Job.delete_all

        SimpleJob.runs = 0
      end

      describe "running a job" do
        before do
          @worker = worker_create(:max_run_time => 1.second)
        end

        it "should fail after Worker.max_run_time" do
          @job = Delayed::Job.create :payload_object => LongRunningJob.new
          @worker.run(@job)
          @job.reload.last_error.should =~ /expired/
          @job.attempts.should == 1
        end

        it "should be destroyed if successful" do
          @job=job_create
          @worker.run(@job)

          Delayed::Job.count.should == 0
        end
      end #b.d.d

      context "worker prioritization" do
        before(:each) do
          @worker = worker_create(:max_priority => 5, :min_priority => -5, :quiet => true)
        end

        it "should only work_off jobs that are >= min_priority" do
          SimpleJob.runs.should == 0

          job_create(:priority => -10)
          job_create(:priority => 0)
          @worker.work_off

          SimpleJob.runs.should == 1
        end

        it "should only work_off jobs that are <= max_priority" do
          SimpleJob.runs.should == 0

          job_create(:priority => 10)
          job_create(:priority => 0)

          @worker.work_off

          SimpleJob.runs.should == 1
        end
      end #b.d.c

      context "while running with locked and expired jobs" do
        before(:each) do
          @worker = worker_create(:name => 'worker1', :max_run_time => 2.minutes)
          @worker.name.should == 'worker1'
        end

        it "should not run jobs locked by another worker" do
          Delayed::Job.count.should == 0
          @job = job_create(:locked_by => 'other_worker', :locked_at => (Delayed::Job.db_time_now - 1.minutes))
          @job.locked_by.should == 'other_worker'
          lambda { @worker.work_off }.should_not change { SimpleJob.runs }
        end

        it "should run open jobs" do
          job_create
          lambda { @worker.work_off }.should change { SimpleJob.runs }.from(0).to(1)
        end

        it "should run expired jobs" do
          expired_time = Delayed::Job.db_time_now - (1.minutes + Delayed::Worker.max_run_time)
          job_create(:locked_by => 'other_worker', :locked_at => expired_time)
          lambda { @worker.work_off }.should change { SimpleJob.runs }.from(0).to(1)
        end

        it "should run own jobs" do
          job_create(:locked_by => @worker.name, :locked_at => (Delayed::Job.db_time_now - 1.minutes))
          lambda { @worker.work_off }.should change { SimpleJob.runs }.from(0).to(1)
        end
      end #b.d.c

      context "destroying failing jobs" do
        before do
          @worker = worker_create(:destroy_failed_jobs => true, :max_attempts => 5)
          @job = job_create :payload_object => ErrorJob.new
        end

        it "should re-schedule jobs after failing" do
          @worker.run(@job)
          @job.reload
          @job.last_error.should =~ /did not work/
          @job.last_error.should =~ /sample_jobs.rb:8:in `perform'/
          @job.attempts.should == 1
          @job.run_at.should > Delayed::Job.db_time_now - 10.minutes
          @job.run_at.should < Delayed::Job.db_time_now + 10.minutes
        end

        it "should not raise an error for a serialization error" do
          @job = job_create
          @job.update_attributes(:handler => "--- !ruby/object:JobThatDoesNotExist {}")
          @worker.run(@job)
        end
      end #b.d.c

      context "keeping failing jobs" do
        before do
          @worker = worker_create(:destroy_failed_jobs => false, :max_attempts => 1)
          @job = job_create :payload_object => ErrorJob.new
        end

        it "should record last_error when destroy_failed_jobs = false, max_attempts = 1" do
          @worker.run(@job)
          @job.reload
          @job.last_error.should =~ /did not work/
          @job.last_error.should =~ /worker_spec.rb/
          @job.attempts.should == 1
          @job.failed_at.should_not be_nil
        end
      end #b.d.c

      context "reschedule" do
        before do
          @worker = worker_create(:max_attempts => 3)
        end

        share_examples_for "any failure more than Worker.max_attempts times" do
          context "when the job's payload has an #on_permanent_failure hook" do
            before do
              @job = job_create :payload_object => OnPermanentFailureJob.new
              @job.payload_object.should respond_to :on_permanent_failure
            end

            it "should run that hook" do
              @job.payload_object.should_receive :on_permanent_failure
              Delayed::Worker.max_attempts.times { @worker.reschedule(@job) }
            end
          end

          context "when the job's payload has no #on_permanent_failure hook" do
            # It's a little tricky to test this in a straightforward way, 
            # because putting a should_not_receive expectation on 
            # @job.payload_object.on_permanent_failure makes that object
            # incorrectly return true to 
            # payload_object.respond_to? :on_permanent_failure, which is what
            # reschedule uses to decide whether to call on_permanent_failure.  
            # So instead, we just make sure that the payload_object as it 
            # already stands doesn't respond_to? on_permanent_failure, then
            # shove it through the iterated reschedule loop and make sure we
            # don't get a NoMethodError (caused by calling that nonexistent
            # on_permanent_failure method).

            before do
              @job = job_create
              @job.payload_object.should_not respond_to(:on_permanent_failure)
            end

            it "should not try to run that hook" do
              lambda do
                Delayed::Worker.max_attempts.times { @worker.reschedule(@job) }
              end.should_not raise_exception(NoMethodError)
            end
          end #b.d.c.s.c
        end #b.d.c.s

        context "and we want to destroy jobs" do
          before do
            @worker = worker_create(:destroy_failed_jobs => true)
            @job = job_create
          end

          it_should_behave_like "any failure more than Worker.max_attempts times"

          it "should be destroyed if it failed more than Worker.max_attempts times" do
            Delayed::Worker.destroy_failed_jobs.should == true
            @job.should_receive(:destroy)
            Delayed::Worker.max_attempts.times { @worker.reschedule(@job) }
            @job.attempts.should == Delayed::Worker.max_attempts
          end

          it "should not be destroyed if failed fewer than Worker.max_attempts times" do
            @job.should_not_receive(:destroy)
            (Delayed::Worker.max_attempts - 1).times { @worker.reschedule(@job) }
            @job.attempts.should == (Delayed::Worker.max_attempts - 1)
          end

          it "should be destroyed if force sent true" do
            @job.should_receive(:destroy)
            @worker.reschedule(@job,nil,true)
          end
        end #b.d.c.c

        context "and we don't want to destroy jobs" do
          before do
            @worker = worker_create(:destroy_failed_jobs => false, :max_attempts => 3)
            @job = job_create
          end

          it_should_behave_like "any failure more than Worker.max_attempts times"

          it "should be failed if it failed more than Worker.max_attempts times" do
            @job.reload.failed_at.should == nil
            Delayed::Worker.max_attempts.times { @worker.reschedule(@job) }
            @job.reload.failed_at.should_not == nil
          end

          it "should not be failed if it failed fewer than Worker.max_attempts times" do
            (Delayed::Worker.max_attempts - 1).times { @worker.reschedule(@job) }
            @job.reload.failed_at.should == nil
          end
        end
      end
    end #b.d
  end #b
end
