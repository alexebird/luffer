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
  (:use [luffer.models :only [plays doc-for-elasticsearch]]
        [luffer.util :only [parse-int secs]]))

;; Elasticsearch
(def ^:private es-conn (es/connect (System/getenv "ES_URL")))
(def ^:private plays-queue "pts-exporter-queue")
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

(defn- select-plays [query]
  (let [play-models (-> query exec)]
    (map doc-for-elasticsearch play-models)))

; TODO build-query needs to select by time buckets - UTC timestamps
; Can we group Plays by track_id? Ignoring the other fields in play that would complicate the grouping.
; Then add a count field that gets indexed to represent the number in the bucket.
(defn- build-query [[start-id stop-id]]
  (-> (select* plays)
      (order :id :ASC)
      (where {:id [<  stop-id]})
      (where {:id [>= start-id]})
      ;(as-sql)
      ))

;(defn- build-query-dates [[start-date stop-date]]
  ;(-> (select* :plays)
      ;(fields :track_id :created_at)
      ;(order :created_at :ASC)
      ;(where {:created_at [>= (clojure.instant/read-instant-timestamp start-date)]})
      ;(where {:created_at [<  (clojure.instant/read-instant-timestamp stop-date)]})
      ;(group :track_id)))

(defn- build-query-dates [[start-date stop-date]]
  (-> (select*
        (raw (str "(SELECT plays.track_id, plays.created_at FROM plays WHERE (plays.created_at >= timestamp '" start-date "') AND (\"plays\".\"created_at\" < timestamp '" stop-date "')) AS foo")))
      (fields "foo.track_id" (raw "COUNT(foo.track_id) AS cnt"))
      ;(aggregate (count :*) :cnt :track_id)
      (group "foo.track_id")))

;(defn- build-query-dates [[start-date stop-date]]
  ;(-> (select* (table
                 ;(-> (select* :plays)
                     ;(fields :track_id :created_at)
                     ;(where {:created_at [>= (clojure.instant/read-instant-timestamp start-date)]})
                     ;(where {:created_at [<  (clojure.instant/read-instant-timestamp stop-date)]}))
                 ;:foo))
      ;(fields :track_id)
      ;(aggregate (count :*) :cnt :track_id)
      ;(group :track_id)
      ;))

;(defn- build-query-dates [[start-date stop-date]]
  ;(exec-raw [(slurp "group_by_tracks.sql") [start-date stop-date]] ))


;SELECT "foo"."track_id", COUNT("foo"."track_id") AS count
;FROM (
    ;SELECT "plays"."track_id", "plays"."created_at"
    ;FROM "plays"
    ;WHERE
      ;("plays"."created_at" >= timestamp '2016-06-01T00:00:00Z')
      ;AND ("plays"."created_at" < timestamp '2016-07-01T00:00:00Z')
    ;-- ;
;) AS foo
;GROUP BY "foo"."track_id";

(defn- get-documents-for-work [work]
  (select-plays (build-query work)))

(defn- bulk-index-plays [index docs]
  (if-not (empty? docs)
    (esbulk/bulk-with-index-and-type es-conn index "play" (esbulk/bulk-index docs))))

(defn- parse-work [raw]
  (if raw
    (map parse-int (str/split raw #"-"))
    nil))

(defn- dequeue-work! []
  (parse-work (wcar* (redis/rpop plays-queue))))

(defn- print-work [worker-id [start-id stop-id :as work]]
  (if work
    (println (format "worker %d exporting [%,d - %,d)" worker-id start-id stop-id))))

(defn- do-work [i callback]
  (let [work (dequeue-work!)]
    (print-work i work)
    (if work
      (let [timing (secs (callback work))]
        (wcar* (redis/incr "export-count"))
        (wcar* (redis/incrbyfloat "export-timing" timing)))
      (Thread/sleep 250)))
  nil)

(defn- worker-loop [i callback]
  (doall (repeatedly #(do-work i callback))))


;; ________       ______ __________
;; ___  __ \___  ____  /____  /__(_)______
;; __  /_/ /  / / /_  __ \_  /__  /_  ___/
;; _  ____// /_/ /_  /_/ /  / _  / / /__
;; /_/     \__,_/ /_.___//_/  /_/  \___/

(defn size-up-bulk-payload [size]
  (format "%.2fM" (/ (count (json/encode (esbulk/bulk-index (get-documents-for-work [1 size])))) (* 1024.0 1024.0))))

(defn run-workers [concurrency index]
  (println (format "starting workers concurrency=%d" concurrency))
  (reset! futures
    (doall
      (map
        (fn [i] (future (worker-loop i #(bulk-index-plays index (get-documents-for-work %)))))
        (range concurrency)))))
