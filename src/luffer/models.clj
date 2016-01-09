(ns luffer.models
  (:require [clojure.string  :as str]
            [korma.core      :refer [select]]
            [korma.db        :refer [defdb postgres]]
            [clj-time.core   :as time]
            [clj-time.format :as timefmt]))

;; :connection-uri can be used as well
;(defdb db (postgres {:db (System/getenv "PG_DATABASE")
                     ;:host (or (System/getenv "PG_PORT_5432_TCP_ADDR") (System/getenv "PG_HOST"))
                     ;:port (or (System/getenv "PG_PORT_5432_TCP_PORT") (System/getenv "PG_PORT") 5432)
                     ;:password (System/getenv "PG_PASSWORD")
                     ;:user (System/getenv "PG_USER")}))

(defdb db (postgres {:connection-uri (System/getenv "PG_URL")}))

(declare plays users tracks shows tours venues api_clients)

(korma.core/defentity plays
  (korma.core/entity-fields :id :source :created_at :ip_address :play_event_id)
  (korma.core/belongs-to users {:fk :user_id})
  (korma.core/belongs-to tracks {:fk :track_id})
  (korma.core/belongs-to api_clients {:fk :api_client_id}))

(korma.core/defentity users
  (korma.core/entity-fields :id :username :email :created_at)
  (korma.core/has-many plays))

(korma.core/defentity tracks
  (korma.core/entity-fields :id :remote_id :title :slug :position
                            :position_in_set :position_in_show :duration :set
                            :set_name :set_index :mp3 :unique_slug :source :created_at)
  ; :string_id => :unique_slug
  ; :set_data [
  ;   :string_id => :show.date$:set_name
  ;   :id => :show.id$:set_name
  ;   :index => :set_index
  ;   :name => :set
  ;   :full_name => :set_name
  ;  ]
  (korma.core/has-many plays)
  (korma.core/belongs-to shows {:fk :show_id}))

(korma.core/defentity shows
  (korma.core/entity-fields :id :remote_id :date :duration :sbd :remastered :source :year :era)
  ; :string_id => :date
  ; :year => :date substring
  ; :era => some kind of mapping
  (korma.core/has-many tracks)
  (korma.core/belongs-to tours {:fk :tour_id})
  (korma.core/belongs-to venues {:fk :venue_id}))

(korma.core/defentity tours
  (korma.core/entity-fields :id :name :slug :starts_on :ends_on :source)
  ; :string_id => :slug
  (korma.core/has-many shows))

(korma.core/defentity venues
  (korma.core/entity-fields :id :name :past_names :location :slug :source)
  ; :string_id => :slug
  ; :location_point [:lat :lon] => {:latitude.to_f :longitude.to_f}
  (korma.core/has-many shows))

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
(def tours-by-id       (id-doc-map (select tours)))
(def venues-by-id      (id-doc-map (select venues)))
(def shows-by-id       (id-doc-map (select shows)))
(def tracks-by-id      (id-doc-map (select tracks)))
(def api-clients-by-id (id-doc-map (select api_clients)))
(def users-by-id       (id-doc-map (select users)))

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
