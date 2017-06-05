(ns onyx.messaging.protocols.epidemic-subscriber)


(defprotocol EpidemicSubscriber
  (start [this])
  (stop [this])
  (add-assembler [this])
  (update-stream-id [sub stream-id])
  (poll! [this]))