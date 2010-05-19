class SimpleJob
  cattr_accessor :runs; self.runs = 0
  def perform; @@runs += 1; end
end

class ErrorJob
  cattr_accessor :runs; self.runs = 0
  def perform; raise 'did not work'; end
end             

class LongRunningJob
  def perform; sleep 250; end
end

class OnPermanentFailureJob < SimpleJob
  def on_permanent_failure
  end
end

class NamedJob < Struct.new(:perform)
  def display_name
    'named_job'
  end
end

module M
  class ModuleJob
    cattr_accessor :runs; self.runs = 0
    def perform; @@runs += 1; end    
  end
end
