(ns onyx.messaging.epidemic-messenger
  (:require [com.stuartsierra.component :as component]))

(defn handle-epidemic-messages [])
(defrecord EpidemicMessenger [messaging-group peer-config monitoring publication-group publications virtual-peers
                              send-idle-strategy compress-f publication-pool short-ids]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Started Epidemic Aeron Messenger")
    (let [publication-pool (:epidemic-publication-pool messaging-group)
          send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f messaging-group)
          virtual-peers (:virtual-peers messaging-group)
          decompress-f (:decompress-f messaging-group)]
      (assoc component
        :messaging-group messaging-group
        :publication-pool publication-pool
        :send-idle-strategy send-idle-strategy
        :compress-f compress-f
        :decompress-f decompress-f)))
  (stop [component]
    (assoc component
      :messaging-group nil
      :publication-pool nil
      :send-idle-strategy nil
      :compress-f nil
      :decompress-f nil)))

(defn epidemic-messenger [messaging-group peer-config])


