(ns onyx.messaging.protocols.epidemic-messenger)



(defprotocol EpidemicMessenger
  (start [this])
  (stop [messenger])
  (id [messenger])
  (info [messenger])
  (update-subscriber [messenger sub-info incoming-ch])
  (update-publisher [messenger pub-infos])
  (offer-log-event! [messenger log-event]))
