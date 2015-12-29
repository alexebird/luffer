(ns luffer.util
  (:gen-class))

(defn parse-int [s]
  (Integer. (re-find  #"\d+" s)))
