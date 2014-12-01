(ns luffer.core
  (:gen-class)
  (:require [clojure.string :as str])
  (:require [cheshire.core :refer :all])
  (:require [clj-http.client :as client])
  (:use [clojure.pprint]
        [korma.core]
        [korma.db]
        [clojure.tools.trace]))

(def http-auth {:basic-auth  "bird:fqKyt6muengOEaatv"})
(def play-counter  (atom 0))
(def batch-counter (atom 1))

(defdb db (postgres {:db "pts_dev"
                     :user "ptsuser"
                     ;:password "kormapass"
                     ;; optional keys
                     ;:delimiters ""
                     }))


(declare plays users tracks shows tours venues api_clients)

;; play model
;t.integer  "user_id"
;t.integer  "track_id"
;t.integer  "api_client_id"
;t.integer  "play_event_id"
;t.text     "original_play_data"
;t.string   "source"
;t.string   "ip_address"
;t.datetime "created_at"
;t.datetime "updated_at"

(defentity plays
  (entity-fields :id :user_id :track_id :api_client_id :source :created_at)
  (belongs-to users {:fk :user_id})
  (belongs-to tracks {:fk :track_id})
  (belongs-to api_clients {:fk :api_client_id}))


;; user model
;t.string   "username"
;t.string   "email"
;t.datetime "created_at"

(defentity users
  (entity-fields :id :username :email :created_at)
  (has-many plays))


;; track model
;t.integer  "show_id"
;t.integer  "remote_id"
;t.string   "title"
;t.string   "slug"
;t.integer  "position"
;t.decimal  "position_in_set"
;t.decimal  "position_in_show"
;t.integer  "duration"
;t.string   "set"
;t.string   "set_name"
;t.integer  "set_index"
;t.string   "mp3"
;t.string   "unique_slug"
;t.string   "source"
;t.datetime "created_at"

(defentity tracks
  (entity-fields :id :show_id :remote_id :title :slug :position :position_in_set :position_in_show :duration :set :set_name :set_index :mp3 :unique_slug :source :created_at)
  (has-many plays)
  (belongs-to shows {:fk :show_id}))

;; show model
;t.integer  "tour_id"
;t.integer  "venue_id"
;t.integer  "remote_id",  :null => false
;t.string   "date",       :null => false
;t.integer  "duration"
;t.boolean  "sbd"
;t.boolean  "remastered"
;t.string   "source",     :null => false

(defentity shows
  (entity-fields :id :tour_id :venue_id :remote_id :date :duration :sbd :remastered :source)
  (has-many tracks)
  (belongs-to tours {:fk :tour_id})
  (belongs-to venues {:fk :venue_id}))


;; tour model
;t.integer  "remote_id",   :null => false
;t.string   "name",        :null => false
;t.integer  "shows_count"
;t.string   "slug",        :null => false
;t.string   "starts_on"
;t.string   "ends_on"
;t.string   "source",      :null => false

(defentity tours
  (entity-fields :id :remote_id :name :shows_count :slug :starts_on :ends_on :source)
  (has-many shows))


;; venue model
;t.integer  "remote_id",   :null => false
;t.string   "name",        :null => false
;t.text     "past_names"
;t.decimal  "latitude"
;t.decimal  "longitude"
;t.integer  "shows_count"
;t.string   "location",    :null => false
;t.string   "slug",        :null => false
;t.string   "source",      :null => false

(defentity venues
  (entity-fields :id :remote_id :name :past_names :latitude :longitude :shows_count :location :slug :source)
  (has-many shows))


;; api_client model

(defentity api_clients
  (entity-fields :id :name :description)
  (has-many plays)
  (belongs-to users {:fk :user_id}))

;; queries
;(select plays (order :created_at) (limit 3))

(defn id-doc-map
  "map ids to entities"
  [coll]
  (reduce (fn [m ent]
            (assoc m (:id ent) ent))
          {}
          coll))

;; get all static data
(def tours-by-id  (id-doc-map (select tours)))
(def venues-by-id (id-doc-map (select venues)))
(def shows-by-id  (id-doc-map (select shows)))
(def tracks-by-id (id-doc-map (select tracks)))
(def api-clients-by-id (id-doc-map (select api_clients)))
(def users-by-id (id-doc-map (select users)))

;; idea: use transform to make column names unique

(defn join-by-id
  "Join in-memory models. Iterate the primary collection and set a key for each matching lookup-collection model."
  ([pcoll lcoll pkey lkey] (join-by-id pcoll lcoll pkey lkey nil))
  ([pcoll lcoll pkey lkey join-fn]
     (into {} (map (fn [[pid pm]]
                     (let [joined-pm (assoc pm pkey (get lcoll (get pm lkey)))]
                       [pid (if join-fn
                              (join-fn joined-pm)
                              joined-pm)]))
                   pcoll))))

(def tracks-joined
  (join-by-id tracks-by-id
              (-> (join-by-id shows-by-id venues-by-id :venue :venue_id)
                  (join-by-id tours-by-id :tour :tour_id))
              :show :show_id
              (fn [track]
                (assoc track :unique_slug (str/join "/" [(get-in track [:show :date])
                                                         (get-in track [:slug])])))))

(defn track-fully-stocked [id]
  (get tracks-joined id))

(defn assoc-user
  "associate the user with the play"
  [play]
  (let [uid (get play :user_id)
        usr (last (get (vec users-by-id) uid))]
    (assoc play :user usr)))

(defn assoc-track
  "associate the track with the play"
  [play]
  (let [tid (get play :track_id)
        t (track-fully-stocked tid)]
    (assoc play :track t)))

(defn assoc-api-client
  "associate the api-client with the play"
  [play]
  (let [acid (get play :api_client_id)
        ac (last (get (vec api-clients-by-id) acid))]
    (assoc play :api_client ac)))

(defn join-play-with-models
  "add user, api_client and track models to the play"
  [play]
  (-> (assoc-user play) (assoc-api-client) (assoc-track)))

(defn write-bulk-record
  "write bulk action line and source document line"
  [stream rec]
  ;; (.write stream "kitties\n")
  (do
    ;; (encode-stream {:index {:_id (:id rec)}} stream)
    (.write stream "{\"index\":{}}\n")
    (encode-stream rec stream)
    (.write stream "\n"))
  )

(defn select-in-batches
  ([query start-id stop-id size batch-handler]
     (let [query   (-> query (order :id :ASC) (limit size))
           query   (if stop-id (-> query (where {:id [< stop-id]})) query)
           query-fn exec
           records (-> query (where {:id [>= start-id]}) query-fn)
           internal-batch-fn (fn [records]
                               (when batch-handler
                                 (batch-handler records)))
           ]
       (if-not (empty? records)
         (internal-batch-fn records))
       (loop [recs records]
         (when-let [recs (not-empty (-> query (where {:id [> (:id (last recs))]}) query-fn))]
           (internal-batch-fn recs)
           (recur recs))))))

(defn write-records-to-file [recs fname]
  (with-open [stream (clojure.java.io/writer fname)]
    (doseq [r recs] (write-bulk-record stream r))))

(defn maybe-stop-id [i concurrency maybe-id]
  (if (= (inc i) concurrency)
    nil
    maybe-id))

(defn round-to-nearest-n [n val]
  (+ val (- n (mod val n))))

(defn handle-batch [recs]
  (let [recs (map join-play-with-models recs)
        recs-count (count recs)
        fname (format "./tmp/batch-%d.json" (swap! batch-counter inc))]
    (swap! play-counter + recs-count)
    ;; (println (format "batch(%d) of %,d plays. total=%,d" @batch-counter recs-count @play-counter))
    (write-records-to-file recs fname)))

(defn play-count []
  (-> (exec-raw "SELECT COUNT(*) AS cnt FROM plays" :results) first :cnt))

(defn reset-counters! []
  (reset! batch-counter 1)
  (reset! play-counter 0))

(defn run-parallel-worker [{:keys [wid start-id stop-id size]}]
  (println (format "new worker(%d) [%,d, %,d)" wid start-id stop-id))
  (future
    (select-in-batches (select* plays) start-id stop-id size handle-batch)
    (println (format "worker %d complete: [%,d - %,d) total=%,d" wid start-id stop-id @play-counter))))

(defn make-workers [concurrency workload-size batch-sizes]
  (for [i (range concurrency)]
    (let [start-id (* i workload-size)
          stop-id  (maybe-stop-id i concurrency (+ workload-size start-id))]
      {:wid i :size batch-sizes :start-id start-id :stop-id stop-id :records nil :num nil})))

(defn start-workers []
  (reset-counters!)
  (let [batch-size 10000
        concurrency 8
        cnt (play-count)
        workload-size (round-to-nearest-n batch-size (int (/ cnt concurrency)))
        workers (make-workers concurrency workload-size batch-size)]
    (println (format "there are %,d plays, workload-size is %,d" cnt workload-size))
    (doall (map deref
                (doall (map run-parallel-worker workers))))
    (println (format "total total %,d" @play-counter))))

(defn -main [& args]
  (println "this would be main"))
