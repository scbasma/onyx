(ns onyx.messaging.epidemic.epidemic-messenger
  (:require [com.stuartsierra.component :as component]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.aeron.publication-pool :as pub-pool]))

(def received-message-counter (atom -1))
(defn inc-value []
  (swap! received-message-counter inc))

(defn handle-epidemic-messages [decompress-f virtual-peers buffer offset length header]
  ; will have to handle incoming log events, and write them to the appropriate channel.
  (println "RECEIVED EPIDEMIC MESSAGE")
  (inc-value)
  (println (str "received messages count: " @received-message-counter)))
  ;(println "inside handle-epidemic-messages!"))
  ;(let [msg (protocol/read-log-event-message buffer offset length)]
  ;  (println  (str "received epidemic message: " msg)))

(def stream-pool-base 11)

(defn get-random-stream-number [stream-pool-base peer-count]
  (let [stream-range (range stream-pool-base (+ stream-pool-base peer-count))]
    11))

(defprotocol PEpidemicMessenger
  (update-stream-pool-size [this peer-count])
  (start-epidemic-subscriber [this]))
 ; (shuffle [this])
  ;(shuffle-publisher [this])
  ;(shuffle-subscriber [this]))

(defrecord EpidemicMessenger [peer-config monitoring publication-group publications virtual-peers
                              send-idle-strategy compress-f publication-pool short-ids decompress-f start-subscriber-fn
                              stream-pool-size epidemic-port external-channel epidemic-publisher-stream-id epidemic-subscriber-shutdown
                              epidemic-subscriber-stream-id epidemic-subscriber]
  PEpidemicMessenger
  (update-stream-pool-size [this peer-count]
    (assoc this :stream-pool-size peer-count))

  (start-epidemic-subscriber [this]
    (let [start-subscriber-fn (:start-subscriber-fn this)
          bind-addr (:bind-addr this)
          epidemic-port (:epidemic-port this)
          epidemic-subscriber-stream-id (:epidemic-subscriber-stream-id this)
          epidemic-subscriber-shutdown (:epidemic-subscriber-shutdown this)
          virtual-peers (:virtual-peers this)
          decompress-f (:decompress-f this)
          receive-idle-strategy (:receive-idle-strategy this)]
      (start-subscriber-fn epidemic-subscriber-shutdown bind-addr epidemic-port epidemic-subscriber-stream-id
                           virtual-peers decompress-f receive-idle-strategy handle-epidemic-messages)))



  component/Lifecycle
  (start [{:keys [messaging-group] :as component}]
    (taoensso.timbre/info "Starting Aeron Epidemic Messenger")
    (let [send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f messaging-group)
          virtual-peers (:virtual-peers messaging-group)
          decompress-f (:decompress-f messaging-group)
          start-subscriber-fn (:start-subscriber-fn messaging-group)
          receive-idle-strategy (:receive-idle-strategy messaging-group)
          bind-addr (:bind-addr messaging-group)
          epidemic-port (:port messaging-group)
          external-channel (:external-channel messaging-group)
          stream-pool-size 0
          publication-pool (component/start (pub-pool/new-publication-pool (:opts messaging-group) send-idle-strategy))
          epidemic-publisher-stream-id stream-pool-base
          epidemic-subscriber-shutdown (atom false)
          epidemic-subscriber-stream-id stream-pool-base
          _ (println "Right before starting epidemic-subscriber")
          epidemic-subscriber (start-subscriber-fn epidemic-subscriber-shutdown bind-addr epidemic-port epidemic-subscriber-stream-id
                                                   virtual-peers decompress-f receive-idle-strategy handle-epidemic-messages)]

      (assoc component
        :messaging-group messaging-group
        :publication-pool publication-pool
        :send-idle-strategy send-idle-strategy
        :compress-f compress-f
        :decompress-f decompress-f
        :start-subscriber-fn start-subscriber-fn
        :receive-idle-strategy receive-idle-strategy
        :bind-addr bind-addr
        :epidemic-port epidemic-port
        :external-channel external-channel
        :stream-pool-size stream-pool-size
        :epidemic-subscriber-stream-id epidemic-subscriber-stream-id
        :epidemic-publisher-stream-id epidemic-publisher-stream-id
        :epidemic-subscriber-shutdown epidemic-subscriber-shutdown
        :epidemic-subscriber epidemic-subscriber)))

  (stop [{:keys [publication-pool send-idle-strategy epidimic-subscriber-shutdown compress-f decompress-f] :as component}]
    (taoensso.timbre/info "Stopping Aeron Epidemic Messenger")
    (component/stop publication-pool)
    (reset! epidemic-subscriber-shutdown true)
    (assoc component
      :publication-pool nil
      :send-idle-strategy nil
      :compress-f nil
      :decompress-f nil)))

(defn epidemic-messenger [peer-config]
  (map->EpidemicMessenger {:peer-config peer-config}))


