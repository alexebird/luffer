(defproject luffer "0.1.0"
  :description "PhishTracks Stats data exporting tool"
  :license {:name "MIT License"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [korma "0.4.2"]
                 [org.clojure/java.jdbc "0.3.7"]
                 [postgresql "9.3-1102.jdbc41"]
                 [cheshire "5.5.0"]
                 [clj-http "2.0.0"]
                 [spyscope "0.1.5"]
                 [org.clojure/tools.cli "0.3.3"]
                 [clojurewerkz/elastisch "2.1.0"]
                 [com.taoensso/carmine "2.12.1"]
                 [clj-time "0.11.0"]]
  :main luffer.core)
