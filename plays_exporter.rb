require 'redis'
require 'pry'

class PlaysExporter
  PLAYS_QUEUE = 'pts-plays-queue'

  attr_reader :redis

  def initialize(start, size, stop)
    url = ENV['REDIS_URL']
    @redis = Redis.new(url: url)
    raise("couldn't talk to redis") unless @redis.ping == 'PONG'
    @start, @size, @stop_exclusive = start, size, stop
    puts "work queue on #{url} '#{PLAYS_QUEUE}' size is #{work_queue_size}"
  end

  def run
    enqueue_jobs(@start, @size, @stop_exclusive)
    cleanup
  end

  private def step_seq(start, stop, step)
    (start...stop).step(step)
  end

  def work_queue_size
    @redis.llen(PLAYS_QUEUE)
  end

  private def generate_jobs(batch_start_ids, step, stop)
    batch_start_ids.map do  |start|
      [start, [start + step, stop].min].join('-')
    end
  end

  private def block_if_work_queue_size_above(n)
    while work_queue_size > n
      sleep 0.5
    end
  end

  def delete_queue
    puts "deleting #{PLAYS_QUEUE}"
    @redis.del(PLAYS_QUEUE)
  end

  # stop id is exclusive
  private def enqueue_jobs(play_id_start, batch_size, play_id_stop)
    step_seq(play_id_start, play_id_stop, batch_size).each_slice(5) do |batch_start_ids|
      jobs = generate_jobs(batch_start_ids, batch_size, play_id_stop)
      puts "pushing #{jobs.size} jobs"
      @redis.lpush(PLAYS_QUEUE, jobs)
      block_if_work_queue_size_above(5)
    end
  end

  private def cleanup
    block_if_work_queue_size_above(0)
    delete_queue
  end

  #def foo ; tot=8000000.0/15000 ; timing=@redis.get("export-timing").to_f ; count=@redis.get("export-count").to_i ; avg=timing/count ; puts format("count: %d\navg:   %.2fs\nest:   %.2fm\n\n", count, avg, ((tot-count)*avg)/60.0/20.0) ; nil ; end
  #:foo
  #[16] pry(main)> loop { foo ; sleep 1 }
end
