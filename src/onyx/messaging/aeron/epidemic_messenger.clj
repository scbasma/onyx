(ns onyx.messaging.aeron.epidemic-messenger
  (:require [onyx.messaging.protocols.epidemic-messenger :as em]
            [onyx.messaging.protocols.epidemic-subscriber :as esub]
            [onyx.messaging.aeron.epidemic-subscriber :refer [new-epidemic-subscriber]]
            [onyx.messaging.protocols.epidemic-publisher :as epub]
            [onyx.messaging.aeron.epidemic-publisher :refer [new-epidemic-publisher]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [debug info warn]]))

(deftype AeronEpidemicMessenger [peer-config messenger-group monitoring
                                 ^:unsynchronized-mutable publisher
                                 ^:unsynchronized-mutable subscriber]
  em/EpidemicMessenger
  (start [this]
    (info "Starting Aeron Epidemic Messenger...")
    (AeronEpidemicMessenger. peer-config messenger-group monitoring
                             (em/update-publisher this {:stream-id 1001 :site {:address "localhost" :port 40199} :peer-id 1111})
                             (em/update-subscriber this {:stream-id 1001 :site {:address "localhost" :port 40199}  :peer-id 1111})))


  (stop [messenger]
    (info "Stopping Aeron Epidemic Messenger...")
    (when subscriber (esub/stop subscriber))
    (when publisher (epub/stop publisher))
    (set! subscriber nil)
    (set! publisher nil))

  (info [this])
  (update-subscriber [messenger sub-info]
    (assert sub-info)
    (set! subscriber
          (esub/update-stream-id
            (or subscriber (esub/start (new-epidemic-subscriber
                                         (:peer-config messenger-group) monitoring 121 sub-info)))
            (:stream-id sub-info))))
  (update-publisher [messenger pub-info]
    (assert pub-info)
    (set! publisher
          (epub/update-stream-id
            (or publisher (epub/start (new-epidemic-publisher
                                        (:peer-config messenger-group) monitoring pub-info)))
            (:stream-id pub-info))))
  (offer-log-event! [messenger log-event]
    (epub/offer-log-event! publisher log-event)))



(defn build-aeron-epidemic-messenger [peer-config messenger-group monitoring]
  (->AeronEpidemicMessenger peer-config messenger-group monitoring nil nil))

(defrecord EpidemicMessenger [peer-config messenger-group monitoring aeron-epidemic-messenger]
  component/Lifecycle
  (start [{:keys [messenger-group monitoring] :as component}]
    (let [aeron-epidemic-messenger (em/start (build-aeron-epidemic-messenger peer-config messenger-group monitoring))]
      (assoc component
        :messenger-group messenger-group
        :monitoring monitoring
        :aeron-epidemic-messenger aeron-epidemic-messenger)))

  (stop [{:keys [messenger-group monitoring aeron-epidemic-messenger] :as component}]
    (when aeron-epidemic-messenger (em/stop aeron-epidemic-messenger))
    (assoc component
      :messenger-group nil
      :monitoring nil
      :aeron-epidemic-messenger nil)))



(defn build-epidemic-messenger [peer-config]
  (->EpidemicMessenger peer-config nil nil nil))

