(ns onyx.messaging.protocols.epidemic-publisher)



(defprotocol EpidemicPublisher
  (start [this])
  (stop [this])
  (offer-log-event! [this log-event])
  (update-stream-id [pub stream-id]))
