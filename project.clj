(defproject luffer "0.1.0"
  :description "PhishTracks Stats data exporting tool"
  :license {:name "MIT License"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [korma "0.4.2"]
                 [org.clojure/java.jdbc "0.3.7"]
                 [postgresql "9.3-1102.jdbc41"]
                 [cheshire "5.5.0"]
                 [clj-http "2.0.0"]
                 [clojurewerkz/elastisch "2.1.0"]]
  :main ^:skip-aot luffer.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
