(ns onyx.epidemics.pick-streams-test
  (:require [clojure.test :refer [is deftest testing]]
  [onyx.test-helper :refer [with-test-env load-config]]
  [onyx.messaging.aeron.epidemic-messenger :refer [pick-streams]]))



(deftest pick-streams-test
  (testing "testing pick streams function")
  (let [pick-from-1 (pick-streams 1)
        pick-from-12 (pick-streams 12)
        pick-from-100 (pick-streams 100)]
    (is (= (count pick-from-1) 2))
    (is (= (count pick-from-12) 2))
    (is (= (count pick-from-100) 2))
    (println (str "first pick: " (first pick-from-1) " second pick: " (second pick-from-1)))
    (println (str "first pick: " (first pick-from-12) " second pick: " (second pick-from-12)))
    (println (str "first pick: " (first pick-from-100) " second pick: " (second pick-from-100)))
    (is (= (first pick-from-1) (second pick-from-1)))
    (is (not= (first pick-from-12) (second pick-from-12)))
    (is (not= (first pick-from-100) (second pick-from-100)))))
