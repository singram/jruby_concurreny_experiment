require 'open-uri'
require 'json'
require 'thread_safe'

require 'logger'
require  'ruby-progressbar'

java_import java.util.concurrent.Executors

class SourceList

  def self.get(size = 1000)
    list = ThreadSafe::Array.new
    list += [*1..size]
    list
  end

end

java.util.concurrent.ThreadPoolExecutor.class_eval do
  java_alias :submit, :submit, [java.util.concurrent.Callable.java_class]
end

class Downloader
  def initialize
    @log = Logger.new STDOUT
    @sourcelist = SourceList.get
    @progressbar = ProgressBar.create(total: @sourcelist.size,
                                      format: "%t: |%B| %c/%C %E %a",
                                      title: "Things"
                                     )
  end

  def progressbar
    @progressbar
  end

  # inner class used to interface with Callable
  class Worker
    include java.util.concurrent.Callable

    def initialize(master, id)
      @id = id
      @master = master
    end

    def call
      process_next(@id)
    end

    def process_next(id)
      return false if id.nil?
      @master.progressbar.increment
      subcount = Random.new.rand(100)
      @master.progressbar.log "Retrieving #{subcount} sub things"
#      puts id
      sleep(1)
      #    @log.debug "processing index: #{id}"
      # Subcount stuff here
      return true
    end

  end


  # main method to define a fixed thread pool and process all indexes
  def work
    executor = java.util.concurrent.Executors::newFixedThreadPool 15
#    Signal.trap('SIGINT') { executor.shutdown }
    while @sourcelist.size > 0
      executor.submit( Worker.new(self, @sourcelist.pop) )
      break if executor.is_shutdown
    end
    executor.shutdown
  end

end



# run it:
data = Downloader.new
data.work
