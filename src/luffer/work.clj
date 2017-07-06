(ns luffer.work
  (:require
    [cheshire.core :as json]
    [clojurewerkz.elastisch.rest :as es]
    [clojurewerkz.elastisch.rest.bulk :as esbulk]
    [korma.core :refer [raw exec-raw aggregate select* fields order where group]])
  (:use
    [luffer.models :as models :only [plays]]))

(def dry-run false)
(def ^:private es-conn (es/connect (System/getenv "ES_URL")))

(defprotocol Work
  "Protocol for work."
  (sql-query [_] "Creates a sql query.")
  (pts-doc [_ doc] "Transforms the document to what PTS expects."))

(defrecord IdsWork [type factor-name start-id end-id]
  Work

  (sql-query [_]
    (-> (select* plays)
        (order :id :ASC)
        (where {:id [>= start-id]})
        (where {:id [<  end-id]})))

  (pts-doc [_ doc]
    (assoc doc
           :all_plays_count 1
           :all_plays_duration (get-in doc [:track :duration]))))

(defn parse-work [work-json]
  (if work-json
    (let [work (json/decode work-json)
          type (get work "type")]
      (case type
        "ids"   (->IdsWork type (get work "factor_name") (get work "start_id") (get work "end_id"))
        ;"dates" (->DatesWork type (get work "factor_name") (get work "start_id") (get work "end_id"))
        :else (println "wtf")))
    nil))

(defn- exec [record]
  (let [q (sql-query record)]
    (if dry-run
      (korma.core/as-sql q)
      (korma.core/exec q))))

(defn- results-to-docs [query-results record]
  (if dry-run
    query-results
    (->>
      query-results
      (map models/es-doc)
      (map #(pts-doc record %1)))))

(defn to-pts-docs [work-str]
  (let [record (parse-work work-str)]
    (results-to-docs (exec record) record)))

(defn bulk-index-docs [index docs]
  (if-not (empty? docs)
    (->>
      (esbulk/bulk-index docs)
      (esbulk/bulk-with-index-and-type es-conn index "track"))))
