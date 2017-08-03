(ns onyx.epidemics.listening-loop-order-test
    (:require [clojure.test :refer :all]
              [clojure.core.async :refer [chan thread <!! >!! close! into]]
              [onyx.peer.communicator :refer [log-entry-listening-loop add-entry-to-state]]
              [onyx.messaging.aeron.epidemic-messenger :refer [parse-entry]]))


(def epidemic-entries [{:message-id 2}
                       {:message-id 6}
                       {:message-id 7}
                       {:message-id 1}
                       {:message-id 9}
                       {:message-id 10}])

(def log-entries [{:message-id 0}
                  {:message-id 3}
                  {:message-id 4}
                  {:message-id 5}
                  {:message-id 8}
                  {:message-id 11}
                  {:message-id 12}
                  {:message-id 13}])

(deftest log-entry-state-test
  (testing "correct state in entry map")
  (let [entry-state {:pos nil :entries []}
        new-state (reduce add-entry-to-state entry-state log-entries)
        new-state-2 (reduce add-entry-to-state new-state epidemic-entries)
        new-state-3 (loop [state new-state-2 entry (first (:entries new-state-2))]
                      (let [old-pos (:pos state)
                            new-state (add-entry-to-state state entry)
                            new-pos (:pos new-state)]
                        (if (not= old-pos new-pos)
                          (recur new-state (first (:entries new-state)))
                          state)))
        new-state-4 (add-entry-to-state new-state-2 (first epidemic-entries))]
    (println "NEW STATE: " new-state)
    (println "NEW STATE-2: " new-state-2)
    (println "NEW-STATE-3: " new-state-3)
    (println "NEW-STATE-4" new-state-4)
    (is (= (add-entry-to-state entry-state (first log-entries)) {:pos 0 :entries []}))))


(deftest log-entry-listening-loop-test
  (testing "correct write order to inbox ch from listening loop")
  (let [epidemic-inbox-ch (chan 100)
        log-inbox-ch (chan 100)
        main-inbox-ch (chan 100)
        sorting-thread (thread (log-entry-listening-loop epidemic-inbox-ch log-inbox-ch main-inbox-ch))]
    ;so lets just feed some log entries into the epidemic inbox ch, in a non-sorted order, then feed entries from log in a sorted order.
    (doall (map #(>!! epidemic-inbox-ch %) epidemic-entries))
    (doall (map #(>!! log-inbox-ch %) log-entries))
    (Thread/sleep 1000)
    (close! epidemic-inbox-ch)
    (close! log-inbox-ch)
    (close! main-inbox-ch)
    (let [inbox-ordered-entries (<!! (into [] main-inbox-ch))]
      (println "Inbox-ordered-entries: " inbox-ordered-entries)
      (is (= inbox-ordered-entries (vec (sort-by :message-id (distinct (concat epidemic-entries log-entries)))))))

    ))
