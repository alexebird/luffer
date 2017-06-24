(ns luffer.core
  (:gen-class)
  (:use [luffer.cli    :only [handle-args]]
        [luffer.worker :only [run-workers]]))

(defn -main [& args]
  (handle-args args
               #(let [{:keys [concurrency work-type index]} %]
                  (luffer.models/populate-model-cache)
                  (run-workers concurrency work-type index))))
