(ns luffer.core
  (:gen-class)
  (:require [clojure.string :as str])
  (:use [clojure.pprint]
        [korma.core]
        [korma.db]))

(defdb db (postgres {:db "pts_dev"
                     :user "ptsuser"
                     ;:password "kormapass"
                     ;; optional keys
                     ;:delimiters ""
                     }))


(declare plays users tracks shows tours venues api_client)

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
  (entity-fields :id :user_id :track_id :source :created_at)
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
(def api_clients-by-id (id-doc-map (select api_clients)))
(def users-by-id (id-doc-map (select users)))

(defn join-by-id
  "Join in-memory models. Iterate the primary collection and set a key for each matching lookup-collection model."
  [pcoll lcoll pkey lkey]
  (into {} (map (fn [[pid pm]]
                  [pid (assoc pm pkey (get lcoll (get pm lkey)))])
                pcoll)))

(pprint (select-keys (-> (join-by-id shows-by-id venues-by-id :venue :venue_id) (join-by-id tours-by-id :tour :tour_id)) [891]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
