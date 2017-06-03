(ns onyx.messaging.epidemic.epidemic-peer-group
  (:require [com.stuartsierra.component :as component]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.aeron.publication-pool :as pub-pool]))

; this record is tasked with tracking the subcriber and publication-groups.

(defprotocol PEpidemicPeerGroup)

(defn stream-pool [peer-count]
  #{11 12})

(defrecord EpidemicPeerGroup [opts virtual-peers send-idle-strategy receive-idle-strategy
                              decompress-f compress-f bind-addr external-addr shutdown publication-pool
                              start-subscriber-fn external-channel port stream-pool subscribers]
  component/Lifecycle
  (start [{:keys [messaging-group] :as component}]
    (let [
          virtual-peers (:virtual-peers messaging-group)
          send-idle-strategy (:send-idle-strategy messaging-group)
          receive-idle-strategy (:receive-idle-strategy messaging-group)
          decompress-f (:decompress-f messaging-group)
          compress-f (:compress-f messaging-group)
          bind-addr (:bind-addr messaging-group)
          external-addr (:external-addr messaging-group)
          shutdown (:shutdown messaging-group)
          publication-pool (:publication-pool messaging-group)
          start-subscriber-fn (:start-subscriber-fn messaging-group)
          external-channel (:external-channel messaging-group)
          port (:port messaging-group)
          ;epidemic-stream-pool need to be implemented after specifications
          stream-pool 11]
          ;subscribers (start-subscriber-fn shutdown bind-addr port stream-pool virtual-peers
          ;                                 decompress-f receive-idle-strategy handle-epidemic-messages]
      (assoc component
        :port port
        :virtual-peers virtual-peers
        :send-idle-strategy send-idle-strategy
        :receive-idle-strategy receive-idle-strategy
        :decompress-f decompress-f
        :bind-addr bind-addr
        :external-addr external-addr
        :external-channel external-channel
        :shutdown shutdown
        :start-subscriber-fn start-subscriber-fn
        :stream-pool stream-pool
        :publication-pool publication-pool
        :subscribers subscribers)))
  (stop [{:keys [port virtual-peers send-idle-strategy receive-idle-strategy decompress-f bind-addr external-addr
                 external-channel shutdown start-subscriber-fn stream-pool publication-pool subscribers] :as component}]
    (assoc component
      :port nil
      :virtual-peers nil
      :send-idle-strategy nil
      :receive-idle-strategy nil
      :decompress-f nil
      :bind-addr nil
      :external-addr nil
      :external-channel nil
      :shutdown nil
      :start-subscriber-fn nil
      :stream-pool nil
      :publication-pool nil
      :subscribers nil)))



(defn epidemic-peer-group [peer-config]
  (map->EpidemicPeerGroup {:peer-config peer-config}))
