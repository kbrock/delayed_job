require 'spec_helper'

describe Delayed::Worker do
  def job_create(opts = {})
    Delayed::Job.create(opts.merge(:payload_object => SimpleJob.new))
  end

  before(:all) do
    Delayed::Worker.send :public, :work_off
  end

  before(:each) do
    @worker = Delayed::Worker.new(:max_priority => nil, :min_priority => nil, :quiet => true)

    Delayed::Job.delete_all
    
    SimpleJob.runs = 0
  end
  
  describe "running a job" do
    it "should fail after Worker.max_run_time" do
      begin
        old_max_run_time = Delayed::Worker.max_run_time
        Delayed::Worker.max_run_time = 1.second
        @job = Delayed::Job.create :payload_object => LongRunningJob.new
        @worker.run(@job)
        @job.reload.last_error.should =~ /expired/
        @job.attempts.should == 1
      ensure
        Delayed::Worker.max_run_time = old_max_run_time
      end
    end
  end
  
  context "worker prioritization" do
    before(:each) do
      @worker = Delayed::Worker.new(:max_priority => 5, :min_priority => -5, :quiet => true)
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
  end

  context "while running with locked and expired jobs" do
    before(:each) do
      @worker.name = 'worker1'
    end
    
    it "should not run jobs locked by another worker" do
      job_create(:locked_by => 'other_worker', :locked_at => (Delayed::Job.db_time_now - 1.minutes))
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
  end
  
  describe "failed jobs" do
    before do
      # reset defaults
      Delayed::Worker.destroy_failed_jobs = true
      Delayed::Worker.max_attempts = 25

      @job = Delayed::Job.enqueue ErrorJob.new
    end

    it "should record last_error when destroy_failed_jobs = false, max_attempts = 1" do
      Delayed::Worker.destroy_failed_jobs = false
      Delayed::Worker.max_attempts = 1
      @worker.run(@job)
      @job.reload
      @job.last_error.should =~ /did not work/
      @job.last_error.should =~ /worker_spec.rb/
      @job.attempts.should == 1
      @job.failed_at.should_not be_nil
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
    #TODO: check that it is rescheduling in the correct amount of time (1 second, ...)    
  end

  context "reschedule" do
    before do
      @job = Delayed::Job.create :payload_object => SimpleJob.new
    end
    
    context "and we want to destroy jobs" do
      before do
        Delayed::Worker.destroy_failed_jobs = true
      end
      
      it "should be destroyed if it failed more than Worker.max_attempts times" do
        @job.should_receive(:destroy)
        Delayed::Worker.max_attempts.times { @worker.reschedule(@job) }
      end
      
      it "should not be destroyed if failed fewer than Worker.max_attempts times" do
        @job.should_not_receive(:destroy)
        (Delayed::Worker.max_attempts - 1).times { @worker.reschedule(@job) }
      end
    end
    
    context "and we don't want to destroy jobs" do
      before do
        Delayed::Worker.destroy_failed_jobs = false
      end
      
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

  context "successful job with destroy turned off" do
    before do
      Delayed::Worker.destroy_successful_jobs = false
    end

    it "should be kept" do
      job=Delayed::Job.enqueue SimpleJob.new
      @worker.run(job)

      Delayed::Job.count.should == 1
    end

    it "should be finished" do
      job = Delayed::Job.enqueue SimpleJob.new

      job.reload.finished_at.should == nil
      @worker.run(job)
      job.reload.finished_at.should_not == nil
    end

    it "should record time when it was picked up by the first worker" do
      job = Delayed::Job.enqueue SimpleJob.new
      job.reload.first_started_at.should == nil
      job.reload.last_started_at.should == nil
      @worker.work_off(1)
      job.reload.first_started_at.should_not == nil
      job.reload.last_started_at.should_not == nil
    end

    #see comment above, remvoed reload from *_started_at
    it "should not update first_started_at on retries" do
      job = Delayed::Job.enqueue ErrorJob.new

      @worker.work_off(1)
      first_started_at = job.reload.first_started_at

      now = Delayed::Job.send :db_time_now
      job.run_at = now
      job.save!

      Delayed::Job.stub!(:db_time_now).and_return(now + 1.hour)
      @worker.work_off(1)

      job.reload.first_started_at.should == first_started_at
    end

    #see comment above, remvoed reload from *_started_at
    it "should update last_started_at on retries" do
      job = Delayed::Job.create :payload_object => ErrorJob.new

      @worker.work_off(1)
      first_started_at = job.reload.first_started_at

      now = Delayed::Job.send :db_time_now
      job.run_at = now
      job.save!

      Delayed::Job.stub!(:db_time_now).and_return(now + 1.hour)
      @worker.work_off(1)

      job.reload.last_started_at.should_not == first_started_at
    end

    it "should clear errors if clear errors turned on" do
      Delayed::Worker.clear_successful_errors=true
      job=Delayed::Job.enqueue SimpleJob.new
      job.update_attributes(:last_error => 'previous error')
      @worker.run(job)
      job.reload.last_error.should == nil
    end

    it "should not clear errors if clear errors turned off" do
      Delayed::Worker.clear_successful_errors=false
      job=Delayed::Job.enqueue SimpleJob.new
      job.update_attributes(:last_error => 'previous error')
      @worker.run(job)
      job.reload.last_error.should == 'previous error'
    end
  end
  
  context "successful jobs with destroy turned on" do
    it "should be destroyed" do
      Delayed::Worker.destroy_successful_jobs = true
      job=Delayed::Job.enqueue SimpleJob.new
      @worker.run(job)

      Delayed::Job.count.should == 0
    end
  end

  it "should ignore ActiveRecord::RecordNotFound errors because they are permanent" do
    Delayed::Job.count.should == 0
    job = ErrorObject.new.send_later(:throw)
    Delayed::Job.count.should == 1
    lambda { @worker.work_off(1) }.should_not raise_error
    Delayed::Job.count.should == 0
  end
  
  
end
