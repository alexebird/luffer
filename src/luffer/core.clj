(ns luffer.core
  (:gen-class)
  (:require [clojure.string :as str])
  (:require [cheshire.core :refer :all])
  (:use [clojure.pprint]
        [korma.core]
        [korma.db]
        [clojure.tools.trace]))

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

(defn join-plays
  "add user, api_client and track models to each play"
  [plays]
  (map #(-> (assoc-user %) (assoc-api-client) (assoc-track)) plays))

(defn write-bulk-play
  "write bulk action line and source document line"
  [stream p]
  (do
    (encode-stream {:index {:_index "plays.exp-1" :_type "play" :_id (:id p)}} stream)
    (.write stream "\n")
    (encode-stream p stream)
    (.write stream "\n")))

(defn write-bulk-plays [stream ps]
  (doseq [p ps] (write-bulk-play stream p)))

(def batchno (atom 1))

(defn select-in-batches
  ([query limit batch-fn] (select-in-batches query limit nil batch-fn))

  ([query limit stop-id batch-fn] (select-in-batches query 0 limit stop-id batch-fn))

  ([query start lim stop-id batch-fn]
     (let [query   (-> query (order :id :ASC) (limit lim))
           query   (if stop-id (-> query (where {:id [< stop-id]})) query)
           records (-> query (where {:id [>= start]}) exec)
           internal-batch-fn (fn [batch batch-no]
                               (when batch-fn
                                 (batch-fn {:batch batch :batchno batch-no})))
           ]
        (if-not (empty? records)
          (internal-batch-fn records @batchno))
       (loop [recs records]
         (when-let [recs (not-empty (-> query (where {:id [> (:id (last recs))]}) exec))]
           (internal-batch-fn recs (swap! batchno inc))
           (recur recs))))))

(defn write-batch-to-file
  "write a damn batch. fname can include a '%d' for the batch number."
  [fname-pattern batch]
  (with-open [stream (clojure.java.io/writer (format fname-pattern (:batchno batch)))]
    (write-bulk-plays stream (:batch batch))))

(def pc (atom 0))

(defn -main [& args]
  (let [batch-size 15000
        concurrency 1
        cnt (-> (exec-raw "SELECT COUNT(*) AS cnt FROM plays" :results) first :cnt)
        quoti (int (/ cnt concurrency))
        id-stop (+ quoti (- batch-size (mod quoti batch-size)))]
    (reset! batchno 1)
    (reset! pc 0)
    (println (format "there are %d plays" cnt))
    (loop [i 0, start 0, stop id-stop]
      (when (< i concurrency)
        (println (format "new future   [%d,%d)" start stop))
        (select-in-batches (select* plays) start batch-size stop
                           (fn [bat]
                             (let [joined-bat (assoc bat :batch (join-plays (:batch bat)))]
                               (swap! pc + (-> bat :batch count))
                               (println (format "did %d plays. total=%d" (-> bat :batch count) @pc))
                               (write-batch-to-file "./tmp/batch-%d.json" joined-bat))))
        (println (format "done with    ^ [%d,%d). total=%d" start stop @pc))
        (recur (inc i) (+ start id-stop) (if (= (inc i) concurrency) nil (+ stop id-stop)))))))
