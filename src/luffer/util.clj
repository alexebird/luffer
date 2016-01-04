(ns luffer.util
  (:gen-class))

(defn parse-int [s]
  (Integer. (re-find  #"\d+" s)))

(defmacro secs
  "Evaluates expr and prints the time it took. Returns the timing value in seconds."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     (/ (double (- (. System (nanoTime)) start#)) 1000000000.0)))

;(defmacro secs-to-redis [expr]
  ;`(let [start# (. System (nanoTime))
         ;ret# ~expr]
     ;(/ (double (- (. System (nanoTime)) start#)) 1000000000.0)))
