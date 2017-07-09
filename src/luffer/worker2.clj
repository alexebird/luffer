(ns luffer.worker2
  (:require
    [taoensso.carmine :as redis :refer [wcar]]))

(def ^:private plays-queue "pts-exporter-queue")
(def ^:private redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})
(def ^:private future-count (atom 0))
(def ^:private job-count (atom 0))

(defmacro wcar* [& body]
  `(redis/wcar redis-conn ~@body))

;(defn print-work [i work]
  ;(printf "%d: %s\n" i work))

(defn- dequeue-job []
  (wcar* (redis/brpop plays-queue 1)))

(defn run-future [work-fn input-work]
  (future
    (do
      (try
        (work-fn input-work @future-count)
        (catch Exception e (do (println e)
                               e)))
      (swap! future-count dec)
      nil)))

(defn- perform-job [input-work work-fn]
  (if (nil? input-work)
    (println "input-work is nil")
    (let [input-work (last input-work)]
      (println "perform-job")
      (swap! future-count inc)
      (swap! job-count inc)
      (printf "job future-count=%d job-count=%d work=%s\n" @future-count @job-count input-work)
      (run-future work-fn input-work))))

(defn run-workers [concurrency index-prefix work-fn]
  (reset! job-count 0)
  (reset! future-count 0)
  (while true
    (println "looping")
    (if (< @future-count concurrency)
      (perform-job (dequeue-job) work-fn)
      (do
        (println "at capacity")
        (Thread/sleep 500))))
  (printf "jobs=%d\n" @job-count))
