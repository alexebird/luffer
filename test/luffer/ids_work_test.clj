(ns luffer.ids-work-test
  (:require [clojure.test :refer :all]
            [korma.core :refer [as-sql]]
            [luffer.ids-work]))

(deftest ids-work-test
  (let [work-json "{\"type\":\"ids\",\"factor_name\":\"1p\",\"start_id\":90,\"end_id\":100}"
        work #luffer.ids_work.IdsWork{:end-id 100 :factor-name "1p" :start-id 90 :type "ids"}]

    (testing "parse parses the work"
      (is (= (luffer.ids-work/parse work-json) work)))

    (testing "sql-query returns a query"
      (is (re-find #"SELECT" (as-sql (luffer.work/sql-query work)))))))
