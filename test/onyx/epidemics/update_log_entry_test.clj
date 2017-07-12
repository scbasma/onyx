(ns onyx.epidemics.update-log-entry-test
    (:require
      [clojure.test :refer [is deftest testing]]
      [onyx.test-helper :refer [with-test-env load-config]]
      [onyx.static.uuid :refer [random-uuid]]
      [onyx.messaging.protocols.epidemic-messenger :as epm]
      [onyx.messaging.aeron.epidemic-messenger :refer [build-aeron-epidemic-messenger]]
      [onyx.extensions :as extensions]
      [clojure.core.async :refer [chan]]))




;(defn build-aeron-epidemic-messenger [peer-config messenger-group monitoring incoming-ch])

(def log-entries [{:data {:log-info "/log/entry-00000000"}}
                  {:data {:log-info "/log/entry-00000001"}}
                  {:data {:log-info "/log/entry-00000002"}}
                  {:data {:log-info "/log/entry-00000003"}}
                  {:data {:log-info "/log/entry-00000004"}}
                  {:data {:log-info "/log/entry-00000005"}}
                  {:data {:log-info "/log/entry-00000006"}}
                  {:data {:log-info "/log/entry-00000007"}}
                  {:data {:log-info "/log/entry-00000008"}}
                  {:data {:log-info "/log/entry-00000009"}}
                  {:data {:log-info "/log/entry-00000010"}}]
                  )

(deftest update-log-entry-test
  (testing "update log entry functions correctly in epidemic messenger")
  (let [config (load-config)
        epidemic-ch (chan 100)
        messenger (build-aeron-epidemic-messenger config nil nil epidemic-ch)]
    (is (nil? (epm/get-all-log-events messenger)))
    (is (= (first log-entries) (epm/update-log-entries messenger (first log-entries))))
    (doall (map #(epm/update-log-entries messenger %) log-entries))
    (is (= (nth log-entries 10) (epm/get-latest-log-event messenger)))
    ))