(ns luffer.worker2
  (:require
    [clojure.string :as str]
    [clj-http.client :as http]
    [clojure.pprint]
    [taoensso.carmine :as redis :refer [wcar]]))

(def ^:private plays-queue "pts-exporter-queue")
(def ^:private redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})
(def ^:private future-count (atom 0))
(def ^:private job-count (atom 0))

(defmacro wcar* [& body]
  `(redis/wcar redis-conn ~@body))

(defn print-work [i work]
  (printf "%d: %s\n" i work))

(defn- dequeue-job []
  (wcar* (redis/brpop plays-queue 1)))

(defn- perform-job [input-work work-fn]
  (if (not (nil? input-work))
    (do
      (println "perform-job")
      (swap! future-count inc)
      (swap! job-count inc)
      (printf "job future-count=%d job-count=%d work=%s\n" @future-count @job-count input-work)
      (future
        (do
          (work-fn input-work @future-count)
          (swap! future-count dec))))))

(defn run-workers [concurrency index-prefix work-fn]
  (reset! job-count 0)
  (reset! future-count 0)
  (while true
    (println "looping")
    (if (< @future-count concurrency)
      (perform-job (dequeue-job) work-fn)
      (do
        (println "no work")
        (Thread/sleep 500))))
  (printf "jobs=%d\n" @job-count))
