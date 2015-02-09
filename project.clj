(defproject luffer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [korma "0.3.0"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [postgresql "9.3-1102.jdbc41"]
                 [cheshire "5.3.1"]
                 [clj-http "1.0.1"]
                 [clojurewerkz/elastisch "2.1.0"]]
  :main ^:skip-aot luffer.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
