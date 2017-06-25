(ns luffer.worker
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [spyscope.core]
            ;[clojure.tools.trace]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint]
            [taoensso.carmine :as redis :refer [wcar]]
            [korma.core :refer [raw exec-raw aggregate select select* fields order exec where group as-sql]]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.bulk :as esbulk])
  (:use [luffer.models :only [plays doc-for-elasticsearch-ids doc-for-elasticsearch-dates]]
        [luffer.util :only [parse-int secs]]))

;; Elasticsearch
(def ^:private es-conn (es/connect (System/getenv "ES_URL")))
(def ^:private plays-queue-ids "pts-exporter-queue-ids")
(def ^:private plays-queue-dates "pts-exporter-queue-dates")
;; Redis
(def ^:private redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})
(defmacro wcar* [& body] `(redis/wcar redis-conn ~@body))
(def ^:private futures (atom []))


;;              .__               __
;; _____________|__|__  _______ _/  |_  ____
;; \____ \_  __ \  \  \/ /\__  \\   __\/ __ \
;; |  |_> >  | \/  |\   /  / __ \|  | \  ___/
;; |   __/|__|  |__| \_/  (____  /__|  \___  >
;; |__|                        \/          \/

(defn- select-plays-dates [query]
  (let [play-models (-> query exec)]
    (map doc-for-elasticsearch-dates play-models)))

(defn- select-plays-ids [query]
  (let [play-models (-> query exec)]
    (map doc-for-elasticsearch-ids play-models)))

(defn- build-query [[start-id stop-id]]
  (-> (select* plays)
      (order :id :ASC)
      (where {:id [<  stop-id]})
      (where {:id [>= start-id]})
      ;(as-sql)
      ))

; TODO build-query needs to select by time buckets - UTC timestamps
; Can we group Plays by track_id? Ignoring the other fields in play that would complicate the grouping.
; Then add a count field that gets indexed to represent the number in the bucket.
(defn- build-query-dates [[start-date stop-date]]
  (let [query (-> (select*
                    (raw (str "(SELECT plays.track_id, plays.created_at FROM plays WHERE (plays.created_at >= timestamp '"
                              start-date
                              "') AND (plays.created_at < timestamp '"
                              stop-date
                              "')) AS foo")))
                  (fields "foo.track_id" (raw "COUNT(foo.track_id) AS all_plays_count") (raw (str "'" stop-date "' AS created_at")))
                  (group "foo.track_id"))]
    ;(println (as-sql query))
    query))

(defn- get-documents-for-work-dates [work]
  (select-plays-dates (build-query-dates work)))

(defn- get-documents-for-work-ids [work]
  (select-plays-ids (build-query work)))

(defn- bulk-index-plays-ids [index docs]
  (if-not (empty? docs)
    (esbulk/bulk-with-index-and-type es-conn index "play" (esbulk/bulk-index docs))))

(defn- bulk-index-plays-dates [index docs]
  (if-not (or (nil? docs) (empty? docs))
    (esbulk/bulk-with-index-and-type es-conn index "play" (esbulk/bulk-index docs))))

(defn- parse-work-ids [raw]
  (if raw
    (map parse-int (str/split raw #"-"))
    nil))

(defn- parse-work-dates [raw]
  (if raw
    (map str (str/split raw #"\$"))
    nil))

(defn- dequeue-work-ids! []
  (parse-work-ids (wcar* (redis/rpop plays-queue-ids))))

(defn- dequeue-work-dates! []
  (parse-work-dates (wcar* (redis/rpop plays-queue-dates))))

(defn- print-work-ids [worker-id [start-id stop-id :as work]]
  (if work
    (println (format "worker %d exporting [%,d - %,d)" worker-id start-id stop-id))))

(defn- print-work-dates [worker-id [start-id stop-id :as work]]
  (if work
    (println (format "worker %d exporting [%s - %s)" worker-id (str start-id) (str stop-id)))))

(defn- do-work [i dequeue-fn print-fn callback]
  (let [work (dequeue-fn)]
    (print-fn i work)
    (if work
      (let [timing (secs (callback work))]
        (wcar* (redis/incr "export-count"))
        (wcar* (redis/incrbyfloat "export-timing" timing)))
      (Thread/sleep 100)))
  nil)

(defn- worker-loop [i dequeue-fn print-fn callback]
  (doall (repeatedly #(do-work i dequeue-fn print-fn callback))))

(defn- get-dequeue-fn [name]
  (case name
    "ids"   dequeue-work-ids!
    "dates" dequeue-work-dates!))

(defn- get-documents-fn [name]
  (case name
    "ids"   get-documents-for-work-ids
    "dates" get-documents-for-work-dates))

(defn- bulk-index-plays-fn [name]
  (case name
    "ids"   bulk-index-plays-ids
    "dates" bulk-index-plays-dates))

(defn- print-work-fn [name]
  (case name
    "ids"   print-work-ids
    "dates" print-work-dates))


;; ________       ______ __________
;; ___  __ \___  ____  /____  /__(_)______
;; __  /_/ /  / / /_  __ \_  /__  /_  ___/
;; _  ____// /_/ /_  /_/ /  / _  / / /__
;; /_/     \__,_/ /_.___//_/  /_/  \___/

;(defn size-up-bulk-payload [size]
  ;(format "%.2fM" (/ (count (json/encode (esbulk/bulk-index (get-documents-for-work [1 size])))) (* 1024.0 1024.0))))

(defn run-workers [concurrency work-type index]
  (println (format "starting workers concurrency=%d work-type=%s index=%s" concurrency work-type index))
  (reset! futures
    (doall
      (map
        (fn [i] (future (worker-loop
                          i
                          (get-dequeue-fn work-type)
                          (print-work-fn work-type)
                          #((bulk-index-plays-fn work-type)
                            index
                            ((get-documents-fn work-type)
                             %)))))
        (range concurrency)))))
