(ns luffer.models
  (:require [clojure.string  :as strng]
            [korma.core      :refer [select where with order limit defentity entity-fields has-many belongs-to]]
            [korma.db        :refer [defdb postgres]]
            [clj-time.core   :as time]
            [clj-time.format :as timefmt])
  (:use [luffer.util :only [parse-int]]))

(declare auto-join-fks)


;;     dMMMMb  dMMMMb  dMP dMP dMP .aMMMb dMMMMMMP dMMMMMP
;;    dMP.dMP dMP.dMP amr dMP dMP dMP"dMP   dMP   dMP
;;   dMMMMP" dMMMMK" dMP dMP dMP dMMMMMP   dMP   dMMMP
;;  dMP     dMP"AMF dMP  YMvAP" dMP dMP   dMP   dMP
;; dMP     dMP dMP dMP    VP"  dMP dMP   dMP   dMMMMMP

(def ^:private eras
  [[[1983 2000] "1.0"]
   [[2002 2004] "2.0"]
   [[2009 2100] "3.0"]])

(defn- get-era [year]
  (last (filter (fn [[[low high] era :as foo]]
                  (and (>= year low) (<= year high)))
                eras)))

(defn- conn-map [pg-uri]
  (let [re (re-pattern (strng/join "(.+)" ["postgresql://" ":" "@" ":" "/" ""])) ]
    (zipmap [:user :password :host :port :db]
            (rest (re-matches re pg-uri)))))

(defdb db (postgres (conn-map (System/getenv "PG_URL"))))

(declare plays users tracks shows tours venues api_clients)

(defentity plays
  (entity-fields :id :source :created_at :ip_address :api_client_id :user_id :track_id))

(defentity users
  (entity-fields :id :username :email :created_at))

(defentity tracks
  (entity-fields :id :title :slug :position :show_id :position_in_set
                 :position_in_show :duration :set :set_name :set_index :mp3
                 :unique_slug :source :created_at))
  ; :string_id => :unique_slug
  ; :set_data [
  ;   :string_id => :show.date$:set_name
  ;   :id => :show.id$:set_name
  ;   :index => :set_index
  ;   :name => :set
  ;   :full_name => :set_name
  ;  ]

(defentity shows
  (entity-fields :id :date :duration :sbd :remastered :source :tour_id :venue_id))

(defentity tours
  (entity-fields :id :name :slug :starts_on :ends_on :source))

(defentity venues
  (entity-fields :id :location :name :past_names :slug :source :latitude :longitude))

(defentity api_clients
  (entity-fields :id :name :description))


(defn- id-doc-map
  "map ids to entities"
  [coll]
  (into {} (map #(vector (:id %) %) coll)))

;(defn join-by-id
  ;"Join in-memory models. Iterate the primary collection and set a key for each matching lookup-collection model."
  ;([pcoll lcoll pkey lkey] (join-by-id pcoll lcoll pkey lkey nil))
  ;([pcoll lcoll pkey lkey join-fn]
     ;(into {} (map (fn [[pid pm]]
                     ;(let [joined-pm (assoc pm pkey (get lcoll (get pm lkey)))]
                       ;[pid (if join-fn
                              ;(join-fn joined-pm)
                              ;joined-pm)]))
                   ;pcoll))))

(defn- venue-with-derived-fields
  "Add fields required by the ES mapping."
  [venue]
  (let [lat (:latitude venue)
        lon (:longitude venue)
        venue (dissoc venue :latitude :longitude)]
    (merge venue
           {:string_id (:slug venue)
            :location_point {:lat lat
                             :lon lon}})))

(defn- venue-for-es-mapping [venue]
  (-> venue
      venue-with-derived-fields))

(defn- tour-with-derived-fields
  "Add fields required by the ES mapping."
  [tour]
  (merge tour
         {:string_id (:slug tour)}))

(defn- tour-for-es-mapping [tour]
  (tour-with-derived-fields tour))

(defn- show-with-derived-fields
  "Add fields required by the ES mapping."
  [{:keys [date] :as show}]
  (let [year (parse-int (re-find #"\d{4}" date))]
    (merge show
           {:string_id date
            :year year
            :era (get-era year)})))

(defn- show-with-tour [show]
  )

  ; :string_id => :date
  ; :year => :date substring
  ; :era => some kind of mapping

(defn- show-for-es-mapping [show]
  (show-with-derived-fields show))

(defonce ^:private models-cache (atom {}))

(def ^:private model-selectors
  [[:api_client_cache #(select api_clients)]
   [:user_cache       #(select users)]
   [:tour_cache       #(map tour-for-es-mapping (select tours))]
   [:venue_cache      #(map venue-for-es-mapping (select venues))]
   [:show_cache       #(map show-for-es-mapping (select shows))]
   [:track_cache      #(select tracks)]])

(defn- send-off-model-populate [func]
  (send-off (agent {}) (fn [_]
                         (id-doc-map (func)))))

(defn- await-populate-models []
  (apply await (vals @models-cache)))

;(def tracks-joined
  ;'(join-by-id
    ;tracks-by-id
    ;(->
      ;(identity shows-by-id)
      ;(join-by-id venues-by-id :venue :venue_id)
      ;(join-by-id tours-by-id :tour :tour_id))
    ;:show
    ;:show_id
    ;#(assoc %
            ;:unique_slug
            ;(strng/join "/" [(get-in % [:show :date]) (get-in % [:slug])]))))

;(defn assoc-track
  ;"associate the track with the play"
  ;[play]
  ;(let [track (get @(:tracks @models-cache) (get play :track_id))]
    ;(assoc
      ;(dissoc play :track_id)
      ;:track track)))

(defn- model-keywords [model-fk]
  (let [str-name (name model-fk)]
    {:child-model-fk        model-fk
     :child-model-name      (keyword (strng/replace str-name #"_id" ""))
     :child-model-cache-key (keyword (strng/replace str-name #"_id" "_cache"))}))

(defn- assoc-model-fk
  "Associate the model referenced by fk with parent-model, and dissoc fk from
  parent-model."
  [parent-model fk]
  (let [{:keys [child-model-cache-key
                child-model-fk
                child-model-name]} (model-keywords fk)]
    (if-let [cached-models-agt (get @models-cache child-model-cache-key)]
      (let [cached-models @cached-models-agt
            child-model-id (get parent-model child-model-fk)
            child-model    (auto-join-fks (get cached-models child-model-id))]
        (assoc
          (dissoc parent-model child-model-fk)
          child-model-name child-model))
      parent-model)))

(defn- model-foreign-keys [model]
  (filter (fn [k]
            (re-matches #"\A:.+_id\z" (str k)))
          (keys model)))


;; HELPERS

(defn- one-play []
  (first (select plays (order :created_at :DESC) (limit 1) (where {:id 7543482}))))

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

(defn auto-join-fks
  "Detect and associate all foreign keys in model."
  [model]
  (reduce
    (fn [model fk]
      (assoc-model-fk model fk))
    model
    (model-foreign-keys model)))
