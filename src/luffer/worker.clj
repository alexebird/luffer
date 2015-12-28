(ns luffer.worker
  (:gen-class)
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [spyscope.core]
            ;[clojure.tools.trace]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint]
            [taoensso.carmine :as car :refer [wcar]]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.bulk :as esbulk]
            [clojurewerkz.elastisch.rest.admin :as esadmin])
  (:use [luffer.models :only [plays join-play-with-models]]))

;; Elasticsearch
(def ^:private es-conn (es/connect (System/getenv "ES_HOST")))
(def ^:private plays-queue "pts-plays-queue")
;; Redis
(def ^:private redis-conn {:pool {} :spec {:uri (System/getenv "REDIS_URL")}})
(defmacro wcar* [& body] `(car/wcar redis-conn ~@body))


;;              .__               __
;; _____________|__|__  _______ _/  |_  ____
;; \____ \_  __ \  \  \/ /\__  \\   __\/ __ \
;; |  |_> >  | \/  |\   /  / __ \|  | \  ___/
;; |   __/|__|  |__| \_/  (____  /__|  \___  >
;; |__|                        \/          \/

(defn- select-plays [query]
  (let [plz (-> query korma.core/exec)]
    (map join-play-with-models plz)))

(defn- build-query [[start-id stop-id]]
  (-> (korma.core/select* plays)
      (korma.core/order :id :ASC)
      (korma.core/where {:id [<  stop-id]})
      (korma.core/where {:id [>= start-id]})))

(defn- get-documents-for-work [work]
  (select-plays (build-query work)))

(defn- bulk-index-plays [index docs]
  (if-not (empty? docs)
    (esbulk/bulk-with-index-and-type es-conn index "play" (esbulk/bulk-index docs))))

(defn- parse-work [raw]
  (if raw
    (map #(Integer/parseInt (re-find #"\d+" %)) (str/split raw #"-"))
    nil))

(defn- dequeue-work! []
  (parse-work (wcar* (car/rpop plays-queue))))

(defn- do-work [callback]
  (if-let [work (dequeue-work!)]
    (callback work)
    (Thread/sleep 250)))

(defn- worker-loop [callback]
  (doall (repeatedly #(do-work callback))))


;; ________       ______ __________
;; ___  __ \___  ____  /____  /__(_)______
;; __  /_/ /  / / /_  __ \_  /__  /_  ___/
;; _  ____// /_/ /_  /_/ /  / _  / / /__
;; /_/     \__,_/ /_.___//_/  /_/  \___/

(defn run-workers [concurrency index]
  (println (format "starting workers concurrency=%d" concurrency))
  (dotimes [_ concurrency]
    (future
      (worker-loop #(bulk-index-plays index (get-documents-for-work %))))))
