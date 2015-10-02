(ns luffer.core-test
  (:require [clojure.test :refer :all]
            [luffer.core :refer :all]))

(deftest cli-options-parsing
  (testing "Do the CLI options get parsed right?"
    (is (= 0 1))))
