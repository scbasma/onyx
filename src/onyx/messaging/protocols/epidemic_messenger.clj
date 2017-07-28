(ns onyx.messaging.protocols.epidemic-messenger)



(defprotocol EpidemicMessenger
  (start [this])
  (stop [messenger])
  (id [messenger])
  (info [messenger])
  (update-subscriber [messenger sub-info incoming-ch])
  (update-publisher [messenger pub-infos])
  (initiate-log-entries [messenger initial-log-entries])
  (update-log-entries [messenger log-event])
  (get-latest-log-event [messenger])
  (get-all-log-events [messenger])
  (get-messenger-id [messenger])
  (offer-log-event! [messenger log-event])
  (subscriber [messenger]))
