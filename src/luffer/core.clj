(ns luffer.core
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [spyscope.core]
            ;[clojure.tools.trace]
            [clojure.tools.cli :refer  [parse-opts]]
            [clojure.pprint]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.bulk :as esbulk]
            [clojurewerkz.elastisch.rest.admin :as esadmin])
  (:use [luffer.models :only [plays join-play-with-models]]
        [luffer.util])
  (:gen-class))




;;              .__               __
;; _____________|__|__  _______ _/  |_  ____
;; \____ \_  __ \  \  \/ /\__  \\   __\/ __ \
;; |  |_> >  | \/  |\   /  / __ \|  | \  ___/
;; |   __/|__|  |__| \_/  (____  /__|  \___  >
;; |__|                        \/          \/

(def ^:private play-counter  (atom 0))
(def ^:private batch-counter (atom 1))
(def ^:private es-index "plays.exp.1")
(def ^:private es-conn (es/connect (str/join "http://" (System/getenv "ES_HOST"))))

(defn- reset-counters! []
  (reset! batch-counter 1)
  (reset! play-counter 0))

(defn- add-bulk-fields
  "add _index and _type fields"
  [play]
  (merge play {:_index es-index :_type "play"}))

(defn- write-bulk-record
  "write bulk action line and source document line"
  [stream rec]
  (do
    (.write stream "{\"index\":{}}\n")
    (json/encode-stream rec stream)
    (.write stream "\n")))

(defn- select-in-batches
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

(defn- write-records-to-file [recs fname]
  (with-open [stream (clojure.java.io/writer fname)]
    (doseq [r recs] (write-bulk-record stream r))))

;(defn- maybe-stop-id [i concurrency maybe-id]
  ;(if (= (inc i) concurrency)
    ;nil
    ;maybe-id))

;(defn- round-up-to-nearest-multiple [val multiple]
  ;(let [remainder #spy/d (mod val multiple)
        ;deficit #spy/d (- multiple remainder)]
    ;(if (== multiple deficit)
      ;val
      ;(+ val deficit))))

(defn- inc-play-counter! [amount]
  (swap! play-counter + amount))

(defn- handle-batch-files [docs]
  (let [docs-count (count docs)
        fname (format "./tmp/batch-%d.json" (swap! batch-counter inc))]
    (inc-play-counter! docs-count)
    (print ".")
    ;(println (format "batch(%d) of %,d plays. total=%,d" @batch-counter docs-count @play-counter))
    (write-records-to-file docs fname)))

(defn- handle-batch-api [docs]
  (let [docs (map add-bulk-fields docs)]
    (inc-play-counter! (count docs))
    (print ".")
    (esbulk/bulk es-conn (esbulk/bulk-index docs))))

;(defn- play-count []
  ;(-> (korma.core/exec-raw "SELECT COUNT(*) AS cnt FROM plays" :results) first :cnt))

(defn- select-in-batches-as-docs
  [query start-id stop-id size batch-fn]
  (let [internal-batch-fn (fn [recs]
                            (when batch-fn
                              (batch-fn (map join-play-with-models recs))))
        ]
    (select-in-batches query start-id stop-id size internal-batch-fn)))


(defn- export-plays [exporter-id [start-id stop-id] batch-size]
  (println (format "new exporter(%d) [%,d, %,d)" exporter-id start-id stop-id))
  ;(future
    ;;(Thread/sleep (* exporter-id 2000)) ;; stagger by 2 sec
    ;(select-in-batches-as-docs (korma.core/select* plays) start-id stop-id batch-size handle-batch-files)
    ;(println (format "exporter %d complete: [%,d - %,d) total=%,d" exporter-id start-id stop-id @play-counter))
    ;)
  )

(defn- batch-deficit [amount batch-size]
  (let [remainder (rem amount batch-size)]
    (if (zero? remainder)
      0
      (- batch-size remainder))))

(defn- range-ize [bins start-id stop-id batch-size]
  (loop [bins bins
         start-id start-id
         stop-id stop-id
         batch-size batch-size
         acc []]
    (if (empty? bins)
      (vec (reverse acc))
      (let [size (* (first bins) batch-size)
            range-stop (+ start-id size)
            range-stop (if (< range-stop stop-id) range-stop stop-id)]
        (recur (rest bins)
               (+ start-id size)
               stop-id
               batch-size
               (vec (cons [start-id range-stop]
                          acc)))))))

(defn- create-exporters [start-id stop-id concurrency batch-size]
  (let [worker-total    (- stop-id start-id)
        rounded-total   (+ worker-total (batch-deficit worker-total batch-size))
        total-batches   (/ rounded-total batch-size)
        per-concurrency (quot total-batches concurrency)
        leftovers       (rem total-batches concurrency)
        bins            (for [c (range 0 concurrency)] per-concurrency)
        bins-with-leftovers  (vec (concat
                                    (map inc (take leftovers bins))
                                    (drop leftovers bins)))]
    (range-ize bins-with-leftovers start-id stop-id batch-size)))



;; ________       ______ __________
;; ___  __ \___  ____  /____  /__(_)______
;; __  /_/ /  / / /_  __ \_  /__  /_  ___/
;; _  ____// /_/ /_  /_/ /  / _  / / /__
;; /_/     \__,_/ /_.___//_/  /_/  \___/


(defn export-concurrently [{:keys [start-id stop-id concurrency batch-size out-dir] :as options}]
  (reset-counters!)
  (.mkdirs (java.io.File. out-dir))
  (println (format "out-dir=%s" out-dir))
  (println (format "starting exporters batch-size=%,d concurrency=%d" batch-size concurrency))

  (let [exporters (create-exporters start-id stop-id concurrency batch-size)]
    (doall (map-indexed #(export-plays %1 %2 batch-size) exporters))
    ))


;;        .__  .__
;;   ____ |  | |__|
;; _/ ___\|  | |  |
;; \  \___|  |_|  |
;;  \___  >____/__|
;;      \/

(def ^:private cli-options
  [;; First three strings describe a short-option, long-option with optional
   ;; example argument description, and a description. All three are optional
   ;; and positional.
   ["-b" "--batch-size BATCH_SIZE" "Number passed to LIMIT in successive Play SELECTs"
    :default 1000
    :parse-fn #(Integer/parseInt %)
    :validate [#(<= 1 % 100000) "Must be in [1,100k]"]]
   ["-s" "--start-id START_ID" "Play id exporting should start from, inclusive"
    :default 0
    :parse-fn #(Integer/parseInt %)
    :validate [#(<= 0 %) "Must be in [0,]"]]
   ["-f" "--stop-id STOP_ID" "Play id exporting should stop at, inclusive"
    :default 0
    :parse-fn #(Integer/parseInt %)
    :validate [#(<= 0 %) "Must be in [0,]"]]
   ["-o" "--out-dir OUT_DIR" "Directory to output exported batch files. Will be created with mkdir -p."
    :validate [#(not (empty? %)) "Must exist"]]
   ["-c" "--concurrency CONCURRENCY" "Number of exporters to run concurrently"
    :default 1
    ;; Specify a string to output in the default column in the options summary
    ;; if the default value's string representation is very ugly
    ;:default-desc "localhost"
    :parse-fn #(Integer/parseInt %)]
   ;; If no required argument description is given, the option is assumed to
   ;; be a boolean option defaulting to nil
   ;[nil "--detach" "Detach from controlling process"]
   ["-v" nil "Verbosity level; may be specified multiple times to increase value"
    ;; If no long-option is specified, an option :id must be given
    :id :verbosity
    :default 0
    ;; Use assoc-fn to create non-idempotent options
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   ["-h" "--help"]])

(defn- usage [options-summary]
  (->> ["This is my program. There are many like it, but this one is mine."
        ""
        "Usage: luffer [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        ""
        "Please refer to the manual page for more information."]
       (str/join \newline)))

(defn- error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn- exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    ;; Handle help and error conditions
    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 0) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    (export-concurrently options)
    ;; Execute program with options
    ;(case (first arguments)
      ;"start" (server/start! options)
      ;"stop" (server/stop! options)
      ;"status" (server/status! options)
      ;(exit 1 (usage summary)))
    ))
