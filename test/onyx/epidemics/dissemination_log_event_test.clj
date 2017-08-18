(ns onyx.epidemics.dissemination-log-event-test
  (:require
    [clojure.test :refer [is deftest testing]]
    [onyx.test-helper :refer [with-test-env load-config]]
    [onyx.static.uuid :refer [random-uuid]]
    [onyx.messaging.protocols.epidemic-messenger :as epm]
    [onyx.messaging.aeron.epidemic-messenger :refer [build-aeron-epidemic-messenger]]
    [onyx.extensions :as extensions]
    [clojure.core.async :refer [chan <!! into close!]]
    [com.stuartsierra.component :as component]
    [onyx.messaging.aeron.embedded-media-driver :as em]))


(def log-entries [{:message-id 0 :log-info "entry-0"}
                  {:message-id 1 :log-info "entry-1"}
                  {:message-id 2 :log-info "entry-2"}
                  {:message-id 3 :log-info "entry-3"}
                  {:message-id 4 :log-info "entry-4"}
                  {:message-id 5 :log-info "entry-5"}
                  {:message-id 6 :log-info "entry-6"}
                  {:message-id 7 :log-info "entry-7"}
                  {:message-id 8 :log-info "entry-8"}
                  {:message-id 9 :log-info "entry-9"}
                  {:message-id 10 :log-info "entry-10"}])

(defn drain-channel [ep-ch]
  (loop [n (count log-entries) received-entries []]
    (if (> n 0)
      (when-let [received-entry (<!! ep-ch)]
        (recur (dec n) (conj received-entries received-entry)))
      received-entries)))

; note: this test will actually start up 3 messengers, as test-env macro will start up a peer-group
; this is needed because I need to start up the aeron media driver
(deftest dissemination-log-event-test-2-peers
  (testing "update log entry functions correctly in epidemic messenger")
  (let [config (load-config)
        onyx-id (random-uuid)
        epidemic-ch-1 (chan 100)
        epidemic-ch-2 (chan 100)
        liveness-timeout 200
        peer-config {:onyx.messaging.aeron/embedded-driver? false
                 :onyx.messaging.aeron/embedded-media-driver-threading :shared
                 :onyx.messaging/peer-port 40199
                 :onyx.messaging/bind-addr "127.0.0.1"
                 :onyx.peer/subscriber-liveness-timeout-ms liveness-timeout
                 :onyx.peer/publisher-liveness-timeout-ms liveness-timeout
                 :onyx.messaging/impl :aeron}

        ;media-driver (component/start (em/->EmbeddedMediaDriver peer-config))
         ]
    ;(try
      (let [
      messenger-1 (build-aeron-epidemic-messenger peer-config nil nil epidemic-ch-1)
      messenger-2 (build-aeron-epidemic-messenger peer-config nil nil epidemic-ch-2)]
        (try
           (is (empty? (epm/get-all-log-events messenger-1)))
           (is (empty? (epm/get-all-log-events messenger-2)))
           (is (= (:message-id (first log-entries)) (:message-id (epm/get-latest-log-event (epm/update-log-entries messenger-1 (first log-entries)))) ))
           (println "after first test!!")
           (when-let [received-entry (<!! epidemic-ch-2)]
             (println "received entry: " received-entry)
            (is (= (:message-id (first log-entries)) (:message-id received-entry)))
            )
           (println "after second test!!")
          ; (epm/update-log-entries messenger-1 (first log-entries))
           (doall (map #(epm/update-log-entries messenger-1 %) log-entries))
           (println "after doall!")
           ;in order to let the messages propagate to messenger
           (Thread/sleep (/ liveness-timeout 2))
           (close! epidemic-ch-1)
           (close! epidemic-ch-2)
           (let [messenger-2-entries (<!! (into [] epidemic-ch-2))]
             (println "Messenger 2 entries from channel: " messenger-2-entries)
             (println "messenger 2 last: " (last messenger-2-entries))
             (println "log-entries last: " (last log-entries))
             (is (= (:message-id (last log-entries) (:message-id (last messenger-2-entries))))))
           (finally
            (epm/stop messenger-1)
            (epm/stop messenger-2))))))
      ;(finally
        ;(component/stop media-driver)))))
