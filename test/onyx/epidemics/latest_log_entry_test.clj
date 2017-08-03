(ns onyx.epidemics.latest-log-entry-test
  (:require [clojure.test :refer :all]
            [onyx.test-helper :refer [with-test-env load-config]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.messaging.protocols.epidemic-messenger :as epm]
            [onyx.extensions :as extensions]
            [clojure.core.async :refer [<!! >!! chan thread go]]))

(defn get-latest-event-from-log [ch]
  (future (loop [log-entries #{}]
            (println "inside loop")
            (if-let [entry (<!! ch)]
              (recur (conj log-entries entry))
              log-entries))))

(deftest ^:smoke latest-log-entry
  (testing "latest log entry returned from epidemic messenger"))
    ;(let [config (load-config)
    ;      onyx-id (random-uuid)
    ;      env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
    ;      peer-config (assoc (:peer-config config) :onyx/tenancy-id onyx-id)
    ;      n-groups 1]
      ;(with-test-env [test-env [1 env-config peer-config]]
      ; (let [
      ;       added-group-cfg (-> peer-config (assoc-in [:onyx.messaging.aeron/embedded-driver?] false))
      ;       in-ch (chan 1)
      ;       out-chan (chan 1)
      ;       log (:log (:env test-env))
      ;       origin (extensions/subscribe-to-log log in-ch)
      ;       origin-read (extensions/read-chunk log :origin nil)
      ;       last-log-entry (thread (loop [log-entries #{}]
      ;                                (println "inside loop")
      ;                                (when-let [entry (<!! in-ch)]
      ;                                  (>!! out-chan entry)
      ;                                  (recur (conj log-entries entry)))))
      ;       new-origin (extensions/subscribe-to-log log in-ch)]
        ; (println (str "latest log entry from log: " (<!! out-chan)))
         ;(Thread/sleep 2000)
        ; (println (str "messenger id: " (epm/get-messenger-id (:aeron-epidemic-messenger (:epidemic-messenger (:peer-group test-env))))))
        ; (println (str "all log events from messenger: " (epm/get-all-log-events (:aeron-epidemic-messenger (:epidemic-messenger (:peer-group test-env))))))
        ; (println "new-origin: " new-origin)
        ; (println "Read chunk: " (extensions/read-chunk log :origin nil)))))

