(ns luffer.work
  (:require
    [cheshire.core :as json]
    [clojurewerkz.elastisch.rest :as es]
    [clojurewerkz.elastisch.rest.bulk :as esbulk]
    [korma.core :refer [raw exec-raw aggregate select* fields order where group]])
  (:use
    [luffer.models :as models :only [plays]]))

(def dry-run false)
;(def dry-run true)
(def ^:private es-conn (es/connect (System/getenv "ES_URL")))

(defprotocol Work
  "Protocol for work."
  (sql-query [_] "Creates a sql query.")
  (pts-doc [_ doc] "Transforms the document to what PTS expects."))

(defrecord IdsWork [type factor-name start-id end-id]
  Work

  (sql-query [_]
    (-> (select* plays)
        (fields :id :created_at :user_id :track_id :ip_address :api_client_id :source)
        (order :id :ASC)
        (where {:id [>= start-id]})
        (where {:id [<  end-id]})))

  (pts-doc [_ doc]
    (assoc doc
           :all_plays_count 1
           :all_plays_duration (get-in doc [:track :duration]))))

(defrecord DatesWork [type min factor factor-name end-time start-time end-time-s start-time-s]
  Work

  ;; TODO build-query needs to select by time buckets - UTC timestamps
  ;; Can we group Plays by track_id? Ignoring the other fields in play that would complicate the grouping.
  ;; Then add a count field that gets indexed to represent the number in the bucket.
  ;; 2017-07-08 - not sure how up-to-date this comment is.
  (sql-query [_]
    (-> (select*
          (raw (str "(SELECT plays.track_id, plays.created_at FROM plays WHERE (plays.created_at >= timestamp '"
                    start-time
                    "') AND (plays.created_at < timestamp '"
                    end-time
                    "')) AS foo")))
        (fields "foo.track_id"
                (raw "COUNT(foo.track_id) AS all_plays_count")
                (raw (str "'" end-time "' AS created_at")))
        (group "foo.track_id")))

  (pts-doc [_ doc]
    (assoc doc
           :all_plays_duration (* (get doc :all_plays_count)
                                  (get-in doc [:track :duration])))))

(defn parse-work [work-json]
  (if work-json
    (let [work (json/decode work-json)
          type (get work "type")]
      (case type
        "ids"   (->IdsWork type
                           (get work "factor_name")
                           (get work "start_id")
                           (get work "end_id"))
        "dates" (->DatesWork type
                             (get work "min")
                             (get work "factor")
                             (get work "factor_name")
                             (get work "end_time")
                             (get work "start_time")
                             (get work "end_time_s")
                             (get work "start_time_s"))
        :else (println "wtf")))
    nil))

(defn exec [record]
  (let [q (sql-query record)
        sql-str (korma.core/as-sql q)]
    (println sql-str)
    (if-not dry-run
      (->> (korma.core/exec q)
           (remove (fn [e] (nil? (get e :track_id))))))))

(defn results-to-docs [query-results record]
  (if dry-run
    query-results
    (->> query-results
         (map models/es-doc)
         (map #(pts-doc record %1)))))

(defn to-pts-docs [work-str]
  (let [record (parse-work work-str)
        results (exec record)]
    ;(println results)
    (if dry-run
      results
      (results-to-docs results record))))

(defn print-bulk-index [index type doc-count first-doc last-doc]
  (printf "%s/%s - %d - [%s, %s]\n"
          index
          type
          doc-count
          (:created_at first-doc)
          (:created_at last-doc)))

(defn bulk-index-docs [index type bulk-size docs]
  (if-not (empty? docs)
    (->>
      (partition bulk-size bulk-size nil docs)
      (map (fn [sub-docs]
             (print-bulk-index index type (count sub-docs) (first sub-docs) (last sub-docs))
             (->> sub-docs
                  esbulk/bulk-index
                  (esbulk/bulk-with-index-and-type es-conn index type))))
      dorun)
    (println "bulk-index-docs: docs is empty")))
