(ns luffer.models
  (:gen-class)
  (:require [clojure.string :as str]
            [korma.core :refer [select]]
            [korma.db :refer [defdb postgres]]))

(defdb db (postgres {:db (System/getenv "PG_DATABASE") :user (System/getenv "PG_USER")}))

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

(korma.core/defentity plays
  (korma.core/entity-fields :id :user_id :track_id :api_client_id :source :created_at)
  (korma.core/belongs-to users {:fk :user_id})
  (korma.core/belongs-to tracks {:fk :track_id})
  (korma.core/belongs-to api_clients {:fk :api_client_id}))


;; user model
;t.string   "username"
;t.string   "email"
;t.datetime "created_at"

(korma.core/defentity users
  (korma.core/entity-fields :id :username :email :created_at)
  (korma.core/has-many plays))


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

(korma.core/defentity tracks
  (korma.core/entity-fields :id :show_id :remote_id :title :slug :position :position_in_set :position_in_show :duration :set :set_name :set_index :mp3 :unique_slug :source :created_at)
  (korma.core/has-many plays)
  (korma.core/belongs-to shows {:fk :show_id}))

;; show model
;t.integer  "tour_id"
;t.integer  "venue_id"
;t.integer  "remote_id",  :null => false
;t.string   "date",       :null => false
;t.integer  "duration"
;t.boolean  "sbd"
;t.boolean  "remastered"
;t.string   "source",     :null => false

(korma.core/defentity shows
  (korma.core/entity-fields :id :tour_id :venue_id :remote_id :date :duration :sbd :remastered :source)
  (korma.core/has-many tracks)
  (korma.core/belongs-to tours {:fk :tour_id})
  (korma.core/belongs-to venues {:fk :venue_id}))


;; tour model
;t.integer  "remote_id",   :null => false
;t.string   "name",        :null => false
;t.integer  "shows_count"
;t.string   "slug",        :null => false
;t.string   "starts_on"
;t.string   "ends_on"
;t.string   "source",      :null => false

(korma.core/defentity tours
  (korma.core/entity-fields :id :remote_id :name :shows_count :slug :starts_on :ends_on :source)
  (korma.core/has-many shows))


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

(korma.core/defentity venues
  (korma.core/entity-fields :id :remote_id :name :past_names :latitude :longitude :shows_count :location :slug :source)
  (korma.core/has-many shows))


;; api_client model

(korma.core/defentity api_clients
  (korma.core/entity-fields :id :name :description)
  (korma.core/has-many plays)
  (korma.core/belongs-to users {:fk :user_id}))

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
  (-> (assoc-user play) assoc-api-client assoc-track))
