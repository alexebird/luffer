(ns luffer.worker
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [spyscope.core]
            ;[clojure.tools.trace]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint]
            [taoensso.carmine :as car :refer [wcar]]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.bulk :as esbulk]
            [clojurewerkz.elastisch.rest.admin :as esadmin])
  (:use [luffer.models :only [plays join-play-with-models]]
        ;[luffer.util   :only [parse-int]]
        )
  (:gen-class))


;;              .__               __
;; _____________|__|__  _______ _/  |_  ____
;; \____ \_  __ \  \  \/ /\__  \\   __\/ __ \
;; |  |_> >  | \/  |\   /  / __ \|  | \  ___/
;; |   __/|__|  |__| \_/  (____  /__|  \___  >
;; |__|                        \/          \/

(def ^:private es-index "plays.exp.1")
(def ^:private es-conn (es/connect (str/join "http://" (System/getenv "ES_HOST"))))

(def ^:private redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})
(defmacro wcar* [& body] `(car/wcar redis-conn ~@body))

(def ^:private plays-queue "pts-plays-queue")

;(defn- add-bulk-fields
  ;"add _index and _type fields"
  ;[play]
  ;(merge play {:_index es-index :_type "play"}))

(defn- handle-batch-api [index docs]
  (let [docs (map add-bulk-fields docs)]
    (print ".")
    (esbulk/bulk-with-index-and-type es-conn index "play" (esbulk/bulk-index docs))))

(defn- select-plays [query]
  (let [plz (-> query korma.core/exec)]
    (map join-play-with-models plz)))

(defn- build-query [[start-id stop-id]]
  (-> (korma.core/select* plays)
      (korma.core/order :id :ASC)
      (korma.core/where {:id [<  stop-id]})
      (korma.core/where {:id [>= start-id]})))

(defn- parse-work [raw]
  (if raw
    (map #(Integer/parseInt (re-find #"\d+" %)) (str/split raw #"-"))
    nil))

(defn- get-work []
  (parse-work (wcar* (car/rpop plays-queue))))

(defn consume-work [callback]
  (loop []
    (if-let [work (get-work)]
      (do
        (println "work:" work)
        (callback work)))
    (println "nil")
    (Thread/sleep 250)
    (recur)))

;(defn- write-bulk-record
  ;"write bulk action line and source document line"
  ;[stream rec]
  ;(do (.write stream "{\"index\":{}}\n")
      ;(json/encode-stream rec stream)
      ;(.write stream "\n")))

;(defn- write-records-to-file [recs fname]
  ;(with-open [stream (clojure.java.io/writer fname)]
    ;(doseq [r recs] (write-bulk-record stream r))))

;(defn- handle-batch-files [docs]
  ;(let [docs-count (count docs)
        ;fname (format "./tmp/batch-%d.json" (swap! batch-counter inc))]
    ;(inc-play-counter! docs-count)
    ;(print ".")
    ;;(println (format "batch(%d) of %,d plays. total=%,d" @batch-counter docs-count @play-counter))
    ;(write-records-to-file docs fname)))


;; ________       ______ __________
;; ___  __ \___  ____  /____  /__(_)______
;; __  /_/ /  / / /_  __ \_  /__  /_  ___/
;; _  ____// /_/ /_  /_/ /  / _  / / /__
;; /_/     \__,_/ /_.___//_/  /_/  \___/

(defn export-in-parallel [{:keys [concurrency index]}]
  (println (format "starting workers concurrency=%d" concurrency))
  (dotimes [_ concurrency]
    (future
      (consume-work #(count (select-plays (build-query %)))))))


;;        .__  .__
;;   ____ |  | |__|
;; _/ ___\|  | |  |
;; \  \___|  |_|  |
;;  \___  >____/__|
;;      \/

(def ^:private cli-options
  [
   ["-i" "--index ES_INDEX" "Elasticsearch index to export to"
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
  (->> ["Plays Exporter."
        ""
        "Usage: plays-exporter [options] action"
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
      (:help options)            (exit 0 (usage summary))
      (not= (count arguments) 0) (exit 1 (usage summary))
      errors                     (exit 1 (error-msg errors)))
    (export-in-parallel (:options options))))
