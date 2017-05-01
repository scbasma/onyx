(ns onyx.messaging.epidemic-messenger
  (:require [com.stuartsierra.component :as component]
            [onyx.messaging.protocol-aeron :as protocol]))


(defn handle-epidemic-messages [decompress-f virtual-peers buffer offset length header]
  ; will have to handle incoming log events, and write them to the appropriate channel.
  ;(println "inside handle-epidemic-messages!")
  (let [msg (protocol/read-log-event-message buffer offset length)]
    (println  (str "received epidemic message: " msg))))

(defrecord EpidemicMessenger [peer-config monitoring publication-group publications virtual-peers
                              send-idle-strategy compress-f publication-pool short-ids]
  component/Lifecycle
  (start [{:keys [messaging-group] :as component}]
    (taoensso.timbre/info "Starting Aeron Epidemic Messenger")
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
    (taoensso.timbre/info "Stopping Aeron Epidemic Messenger")
    (assoc component
      :messaging-group nil
      :publication-pool nil
      :send-idle-strategy nil
      :compress-f nil
      :decompress-f nil)))

(defn epidemic-messenger [peer-config]
  (map->EpidemicMessenger {:peer-config peer-config}))


