;;        .__  .__
;;   ____ |  | |__|
;; _/ ___\|  | |  |
;; \  \___|  |_|  |
;;  \___  >____/__|
;;      \/

(ns luffer.cli
  (:require [clojure.string :as str]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint]))

(def ^:private cli-options
  [
   ["-i" "--index ES_INDEX" "Elasticsearch index to export to"
    :validate [#(not (empty? %)) "Must exist"]]

   ["-c" "--concurrency CONCURRENCY" "Number of exporters to run concurrently"
    :default 1
    ;; Specify a string to output in the default column in the options summary
    ;; if the default value's string representation is very ugly
    ;:default-desc "localhost"
    :parse-fn #(Integer/parseInt %)]

   ;; If no required argument description is given, the option is assumed to
   ;; be a boolean option defaulting to nil
   ;[nil "--detach" "Detach from controlling process"]

   ["-v" nil "Verbosity level; may be specified multiple times to increase value"
    ;; If no long-option is specified, an option :id must be given
    :id :verbosity
    :default 0
    ;; Use assoc-fn to create non-idempotent options
    :assoc-fn (fn [m k _] (update-in m [k] inc))]

   ["-h" "--help"]])

(defn- usage [options-summary]
  (->> ["Plays Exporter."
        ""
        "Usage: plays-exporter [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        ""
        "Please refer to the manual page for more information."]
       (str/join \newline)))

(defn- error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn- exit [status msg]
  (println msg)
  (System/exit status))

(defn handle-args [args callback]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options)            (exit 0 (usage summary))
      (not= (count arguments) 0) (exit 1 (usage summary))
      errors                     (exit 1 (error-msg errors))
      :else                      (callback options))))
