(ns luffer.models
  (:require [clojure.string  :as strng]
            [korma.core      :refer [select where with order limit defentity entity-fields has-many belongs-to]]
            [korma.db        :refer [defdb postgres]]
            [clj-time.core   :as time]
            [clj-time.format :as timefmt])
  (:use [luffer.util :only [parse-int]]))


;; DECLARATIONS

(declare doc-for-elasticsearch)


;;     dMMMMb  dMMMMb  dMP dMP dMP .aMMMb dMMMMMMP dMMMMMP
;;    dMP.dMP dMP.dMP amr dMP dMP dMP"dMP   dMP   dMP
;;   dMMMMP" dMMMMK" dMP dMP dMP dMMMMMP   dMP   dMMMP
;;  dMP     dMP"AMF dMP  YMvAP" dMP dMP   dMP   dMP
;; dMP     dMP dMP dMP    VP"  dMP dMP   dMP   dMMMMMP

(defn- conn-map [pg-uri]
  (let [re (re-pattern (strng/join "(.+)" ["postgresql://" ":" "@" ":" "/" ""])) ]
    (zipmap [:user :password :host :port :db]
            (rest (re-matches re pg-uri)))))

(defdb db (postgres (conn-map (System/getenv "PG_URL"))))

(defentity plays
  (entity-fields :id :source :created_at :ip_address :api_client_id :user_id :track_id))

(defentity users
  (entity-fields :id :username :email :created_at))

(defentity tracks
  (entity-fields :id :show_id :title :slug :mp3 :duration :unique_slug :source :created_at
                 :position :position_in_set :position_in_show
                 :set :set_name :set_index))

(defentity shows
  (entity-fields :id :date :duration :sbd :remastered :source :tour_id :venue_id))

(defentity tours
  (entity-fields :id :name :slug :starts_on :ends_on :source))

(defentity venues
  (entity-fields :id :location :name :past_names :slug :source :latitude :longitude))

(defentity api_clients
  (entity-fields :id :name :description))




(defmulti ^:private add-es-mapping-fields
  "Adds fields to entity which are expected by the Elasticsearch mapping."
  (fn [entity] (:_type entity)))

(defmethod add-es-mapping-fields :track
  [{:keys [unique_slug set_name set_index set show] :as track}]
  (let [date (:date show)
        show_id (:id show)]
    (merge track
           {:string_id unique_slug
            :set_data {:string_id (str date "$" set_name)
                       :id (str show_id "$" set_name)
                       :index set_index
                       :name set
                       :full_name set_name}})))

(defmethod add-es-mapping-fields :venue
  [venue]
  (let [lat (:latitude venue)
        lon (:longitude venue)
        has-point (boolean (and lat lon))
        venue (dissoc venue :latitude :longitude)]
    (merge venue
           {:string_id (:slug venue)
            :location_point (if has-point
                              {:lat (double lat) :lon (double lon)}
                              nil)})))

(defmethod add-es-mapping-fields :tour
  [tour]
  (merge tour
         {:string_id (:slug tour)}))

(def ^:private eras
  [[[1983 2000] "1.0"]
   [[2002 2004] "2.0"]
   [[2009 2100] "3.0"]])

(defn- get-era [year]
  (last (filter (fn [[[low high] era :as foo]]
                  (and (>= year low) (<= year high)))
                eras)))

(defmethod add-es-mapping-fields :show
  [{:keys [date] :as show}]
  (let [year (parse-int (re-find #"\d{4}" date))]
    (merge show
           {:string_id date
            :year year
            :era (last (get-era year))})))

(defmethod add-es-mapping-fields :default [v] v)




(defonce ^:private models-cache (atom {}))

(defmacro infix
  "Use this macro when you pine for the notation of your childhood"
  [infixed]
  (list (second infixed) (first infixed) (last infixed)))

;(defmacro select-with-type-added [model]
  ;(list `select model))

;(defmacro select-with-type-added [model]
  ;`(select ~model))

(defmacro select-with-type-added [model]
  `(map
     #(assoc % :_type (keyword
                        (strng/replace
                          (:name ~model) #"s$" "")))
     (select ~model)))


(def ^:private model-selectors
  [[:api_client_cache #(select-with-type-added api_clients)]
   [:user_cache       #(select-with-type-added users)]
   [:tour_cache       #(select-with-type-added tours)]
   [:venue_cache      #(select-with-type-added venues)]
   [:show_cache       #(select-with-type-added shows)]
   [:track_cache      #(select-with-type-added tracks)]])

(defn- map-id-to-entity
  "Map an entity's :id to itself."
  [coll]
  (into {} (map #(vector (:id %) %) coll)))

(defn- send-off-model-populate [func]
  (send-off (agent {}) (fn [_]
                         (map-id-to-entity (func)))))

(defn- await-populate-models []
  (apply await (vals @models-cache)))





(defn- model-keywords [model-fk]
  (let [str-name (name model-fk)]
    {:child-model-fk        model-fk
     :child-model-name      (keyword (strng/replace str-name #"_id" ""))
     :child-model-cache-key (keyword (strng/replace str-name #"_id" "_cache"))}))

(defn- get-child-model [cached-models-agt parent-model child-model-fk]
  (doc-for-elasticsearch (get @cached-models-agt (get parent-model child-model-fk))))

(defn- assoc-model-fk
  "Associate the model referenced by fk with parent-model, and dissoc fk from
  parent-model."
  [parent-model fk]
  (let [{:keys [child-model-cache-key] :as model-keys} (model-keywords fk)]
    (if-let [cached-models-agt (get @models-cache child-model-cache-key)]
      (let [{:keys [child-model-fk child-model-name]} model-keys]
        (assoc
          (dissoc parent-model child-model-fk)
          child-model-name
          (get-child-model cached-models-agt parent-model child-model-fk)))
      parent-model)))

(defn- model-foreign-keys [model]
  (filter (fn [k]
            (re-matches #"\A:.+_id\z" (str k)))
          (keys model)))

(defn- auto-join-fks
  "Detect and associate all foreign keys in model."
  [model]
  (reduce
    (fn [model fk]
      (assoc-model-fk model fk))
    model
    (model-foreign-keys model)))




;; HELPERS

(defn- one-play []
  (first (select plays (order :created_at :DESC) (limit 1) (where {:id 490987}))))
  ;(first (select plays (order :created_at :DESC) (limit 1) (where {:id 7543482}))))

(defn- one-track []
  (first (select tracks (order :created_at :DESC) (limit 1))))

(defn- one-show []
  (first (select shows (order :created_at :DESC) (limit 1))))

(defn- one-tour []
  (first (select tours (order :created_at :DESC) (limit 1))))





;;     dMMMMb  dMP dMP dMMMMb  dMP     dMP .aMMMb
;;    dMP.dMP dMP dMP dMP"dMP dMP     amr dMP"VMP
;;   dMMMMP" dMP dMP dMMMMK" dMP     dMP dMP
;;  dMP     dMP.aMP dMP.aMF dMP     dMP dMP.aMP
;; dMP      VMMMP" dMMMMP" dMMMMMP dMP  VMMMP"

(defn populate-model-cache []
  (reset!
    models-cache
    (into {} (map (fn [[name func]]
                    [name (send-off-model-populate func)])
                  model-selectors)))
  (await-populate-models))

(defn doc-for-elasticsearch
  "Transform the model into an Elasticsearch document."
  [model]
  (->
    model
    auto-join-fks
    add-es-mapping-fields
    (dissoc :_type)))
