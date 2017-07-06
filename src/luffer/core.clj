(ns luffer.core
  (:gen-class)
  (:use [luffer.cli]
        [luffer.work]
        [luffer.models]
        [luffer.worker2]))

(defn -main [& args]
  (luffer.cli/handle-args args
               #(let [{:keys [concurrency work-type index]} %]
                  ;(luffer.models/populate-model-cache!)
                  (luffer.worker2/run-workers concurrency work-type index))))
