require 'redis'
require 'pry'
require 'twilio-ruby'

class PlaysExporter
  PLAYS_QUEUE = 'pts-exporter-queue'

  attr_reader :redis

  def initialize(start, size, stop)
    url = ENV['REDIS_URL']
    @redis = Redis.new(url: url)
    raise("couldn't talk to redis") unless @redis.ping == 'PONG'
    @twilio = Twilio::REST::Client.new(ENV['TWILIO_SID'], ENV['TWILIO_AUTH_TOKEN'])
    @start, @size, @stop_exclusive = start.to_i, size.to_i, stop.to_i
    puts "work queue on #{url} '#{PLAYS_QUEUE}' size is #{work_queue_size}"
  end

  def run
    enqueue_jobs(@start, @size, @stop_exclusive)
    notify_done
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

  private def notify_done
    @twilio.messages.create(
      from: ENV['TWILIO_NUMBER'],
      to: ENV['ALERT_NUMBER'],
      body: 'That thing you wanted is done.'
    )
  end

  private def cleanup
    block_if_work_queue_size_above(0)
    delete_queue
  end

  def print_timing(concurrency)
    tot = @stop.to_f / @size
    timing = @redis.get('pts-exporter-timing').to_f
    count = @redis.get('pts-exporter-count').to_i
    avg = timing / count
    secs_in_minute = 60.0
    puts format(
      "count: %d\navg:   %.2fs\nest:   %.2fm\n\n",
      count,
      avg,
      ((tot - count) * avg) / secs_in_minute / concurrency.to_f
    )
  end

  def watch_timing(concurrency)
    loop do
      print_timing(concurrency)
      sleep 1
    end
  end
end
