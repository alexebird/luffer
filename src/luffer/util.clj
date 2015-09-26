(ns luffer.util
  (:gen-class))

(defn parse-int [s]
  #spy/d s
  (Integer. (re-find  #"\d+" s)))
