(ns luffer.worker
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [spyscope.core]
            ;[clojure.tools.trace]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint]
            [taoensso.carmine :as car :refer [wcar]]
            [korma.core :refer [select* order exec where]]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.bulk :as esbulk]
            [clojurewerkz.elastisch.rest.admin :as esadmin])
  (:use [luffer.models :only [plays join-play-with-models]]
        [luffer.util :only [parse-int secs]]))

;; Elasticsearch
(def ^:private es-conn (es/connect (System/getenv "ES_URL")))
(def ^:private plays-queue "pts-exporter-queue")
;; Redis
(def ^:private redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})
(defmacro wcar* [& body] `(car/wcar redis-conn ~@body))
(def ^:private futures (atom []))


;;              .__               __
;; _____________|__|__  _______ _/  |_  ____
;; \____ \_  __ \  \  \/ /\__  \\   __\/ __ \
;; |  |_> >  | \/  |\   /  / __ \|  | \  ___/
;; |   __/|__|  |__| \_/  (____  /__|  \___  >
;; |__|                        \/          \/

(defn- select-plays [query]
  (let [plz (-> query exec)]
    (map join-play-with-models plz)))

(defn- build-query [[start-id stop-id]]
  (-> (select* plays)
      (order :id :ASC)
      (where {:id [<  stop-id]})
      (where {:id [>= start-id]})))

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
  (parse-work (wcar* (car/rpop plays-queue))))

(defn- print-work [worker-id [start-id stop-id :as work]]
  (if work
    (println (format "worker %d exporting [%,d - %,d)" worker-id start-id stop-id))))

(defn- do-work [i callback]
  (let [work (dequeue-work!)]
    (print-work i work)
    (if work
      (let [timing (secs (callback work))]
        (wcar* (car/incr "export-count"))
        (wcar* (car/incrbyfloat "export-timing" timing)))
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
