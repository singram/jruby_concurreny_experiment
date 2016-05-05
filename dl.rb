require 'open-uri'
require 'json'
require 'concurrent'
require 'pp'

require 'logger'
require  'ruby-progressbar'

# http://www.nurkiewicz.com/2014/11/executorservice-10-tips-and-tricks.html
# http://ericlondon.com/2014/04/02/jruby-thread-pool-concurrency-example-to-pull-data-from-elasticsearch.html
# https://github.com/jruby/jruby/wiki/Concurrency-in-jruby#thread_safety
# https://github.com/ruby-concurrency/concurrent-ruby

java_import java.util.concurrent.Executors

class PersonList

  def self.get(size = 1000)
    list = Concurrent::Array.new
    list += [*1..size]
    list
  end

end

java.util.concurrent.ThreadPoolExecutor.class_eval do
  java_alias :submit, :submit, [java.util.concurrent.Callable.java_class]
end

class PeopleDownloader

  attr_accessor  :person_count, :people_progressbar, :documents

  def initialize
    @log = Logger.new STDOUT
    @people = PersonList.get
    @person_count = @people.size
    @people_progressbar = ProgressBar.create(total: person_count,
                                             format: "%t: |%B| %c/%C %E %a",
                                             title: "People"
                                            )
    @documents = Concurrent::Array.new
  end

  EXECUTOR_QUEUE_LIMIT = 30
  THREAD_POOL = 15
  # main method to define a fixed thread pool and process all indexes
  def work
    executor = java.util.concurrent.Executors::newFixedThreadPool THREAD_POOL
    # Pump the requests into the queue for the thread pool to act on.
    while !@people.empty?
      if executor.queue.size < EXECUTOR_QUEUE_LIMIT
        executor.submit( PersonWorker.new(self, @people.pop) )
        break if executor.is_shutdown
      end
    end
    @people_progressbar.log("Executor pool populated")
    # Block until queue is drained
    while !executor.queue.empty? do
      sleep 1
    end
    executor.shutdown
    @people_progressbar.refresh

  end

end

# inner class used to interface with Callable
class PersonWorker
  include java.util.concurrent.Callable

  def initialize(master, id)
    @master = master
    @id = id
  end

  def call
    process_next(@id)
  end

  def process_next(id)
    # Process atomic request
    return false if id.nil?
    @master.people_progressbar.increment
    document_count = Random.new.rand(100)
    @master.documents += [*1..document_count]
    sleep 0.2
    # Subcount stuff here
    return true
  end

end


data = PeopleDownloader.new
data.work

puts
puts "------------"
puts "People - #{data.person_count}"
puts "Documents - #{data.documents.size}"
