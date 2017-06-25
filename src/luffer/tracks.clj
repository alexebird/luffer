(ns luffer.tracks)

(defn- assoc-play-counts [play]
  (if-let [all-plays-count (:all_plays_count play)]
    (if-let [track-duration  (get-in play [:track :duration])]
      (merge play {:all_plays_duration (* all-plays-count track-duration)})
      play)
    play))

(defn- flatten-child-docs [doc]
  (->> (map
         (fn [[k new-name]] [new-name (get-in doc k)])
         [[[:created_at]                     :created_at]
          [[:all_plays_count]                :all_plays_count]
          [[:all_plays_duration]             :all_plays_duration]

          [[:track :title]                   :track_title]
          [[:track :duration]                :track_duration]
          [[:track :slug]                    :track_slug]
          [[:track :unique_slug]             :track_unique_slug]
          [[:track :mp3]                     :mp3]

          [[:track :show :date]              :show_date]
          [[:track :show :year]              :year]
          [[:track :show :era]               :era]
          [[:track :show :remastered]        :remastered]
          [[:track :show :sbd]               :sbd]

          [[:track :show :venue :name]       :venue_name]
          [[:track :show :venue :location]   :venue_location]
          [[:track :show :venue :past_names] :venue_past_names]

          [[:track :show :tour :name]        :tour_name]
          [[:track :show :tour :starts_on]   :tour_start_date]
          [[:track :show :tour :ends_on]     :tour_end_date]
          ])
     (into {})))

(defn- add-day [doc]
  (let [day (re-find #"(?<=-)\d{2}(?=-)" (:show_date doc))]
    (assoc doc :day day)))

(defn dense-doc [doc]
  (-> doc
      assoc-play-counts
      flatten-child-docs))
