(ns luffer.core
  (:gen-class)
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [clj-http.client :as http]
            ;[clojure.tools.trace]
            [clojure.pprint]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.bulk :as esbulk]
            [clojurewerkz.elastisch.rest.admin :as esadmin])
  (:use [luffer.models :only [plays join-play-with-models]]))

(def play-counter  (atom 0))
(def batch-counter (atom 1))
(def es-index "plays.exp.1")
(def es-conn (es/connect (str/join "http://" (System/getenv "ES_HOST"))))

(defn add-bulk-fields
  "add _index and _type fields"
  [play]
  (merge play {:_index es-index :_type "play"}))

(defn write-bulk-record
  "write bulk action line and source document line"
  [stream rec]
  (do
    (.write stream "{\"index\":{}}\n")
    (json/encode-stream rec stream)
    (.write stream "\n")))

(defn select-in-batches
  [query start-id stop-id size batch-fn]
  (let [query   (-> query (korma.core/order :id :ASC) (korma.core/limit size))
        query   (if stop-id (-> query (korma.core/where {:id [< stop-id]})) query)
        query-fn korma.core/exec
        records (-> query (korma.core/where {:id [>= start-id]}) query-fn)
        internal-batch-fn (fn [records]
                            (when batch-fn
                              (batch-fn records)))
        ]
    (if-not (empty? records)
      (internal-batch-fn records))
    (loop [recs records]
      (when-let [recs (not-empty (-> query (korma.core/where {:id [> (:id (last recs))]}) query-fn))]
        (internal-batch-fn recs)
        (recur recs)))))

(defn select-in-batches-as-docs
  [query start-id stop-id size batch-fn]
  (let [internal-batch-fn (fn [recs]
                            (when batch-fn
                              (batch-fn (map join-play-with-models recs))))
        ]
    (select-in-batches query start-id stop-id size internal-batch-fn)))

(defn write-records-to-file [recs fname]
  (with-open [stream (clojure.java.io/writer fname)]
    (doseq [r recs] (write-bulk-record stream r))))

(defn maybe-stop-id [i concurrency maybe-id]
  (if (= (inc i) concurrency)
    nil
    maybe-id))

(defn round-to-nearest-n [n val]
  (+ val (- n (mod val n))))

(defn- inc-play-counter! [amount]
  (swap! play-counter + amount))

(defn handle-batch-files [docs]
  (let [docs-count (count docs)
        fname (format "./tmp/batch-%d.json" (swap! batch-counter inc))]
    (inc-play-counter! docs-count)
    (print ".")
    ;; (println (format "batch(%d) of %,d plays. total=%,d" @batch-counter docs-count @play-counter))
    (write-records-to-file docs fname)))

(defn handle-batch-api [docs]
  (let [docs (map add-bulk-fields docs)]
    (inc-play-counter! (count docs))
    (print ".")
    (esbulk/bulk es-conn (esbulk/bulk-index docs))))

(defn play-count []
  (-> (korma.core/exec-raw "SELECT COUNT(*) AS cnt FROM plays" :results) first :cnt))

(defn reset-counters! []
  (reset! batch-counter 1)
  (reset! play-counter 0))

(defn run-parallel-worker [{:keys [wid start-id stop-id size]}]
  (println (format "new worker(%d) [%,d, %,d)" wid start-id stop-id))
  (future
    (Thread/sleep (* wid 2000)) ;; stagger by 2 sec
    (select-in-batches-as-docs (korma.core/select* plays) start-id stop-id size handle-batch-files)
    (println (format "worker %d complete: [%,d - %,d) total=%,d" wid start-id stop-id @play-counter))))

(defn make-workers [concurrency workload-size batch-sizes]
  (for [i (range concurrency)]
    (let [start-id (* i workload-size)
          stop-id  (maybe-stop-id i concurrency (+ workload-size start-id))]
      {:wid i :size batch-sizes :start-id start-id :stop-id stop-id :records nil :num nil})))

(defn start-workers [batch-size concurrency]
  (reset-counters!)
  (let [cnt (play-count)
        workload-size (round-to-nearest-n batch-size (int (/ cnt concurrency)))
        workers (make-workers concurrency workload-size batch-size)]
    (println (format "there are %,d plays, workload-size is %,d" cnt workload-size))
    ;; (doall (map deref
    ;;       ))
    (doall (map run-parallel-worker workers))
    ;; (println (format "total total %,d" @play-counter))
    ))

(defn -main [& args]
  (println "this would be main"))
