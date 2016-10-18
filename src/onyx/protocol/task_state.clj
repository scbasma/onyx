(ns onyx.protocol.task-state)

(defprotocol PTaskStateMachine
  (killed? [this])
  (stop [this])
  (initial-state? [this])
  (advanced? [this])
  (next-replica! [this replica])
  (next-cycle! [this])
  (set-pipeline! [this new-pipeline])
  (get-pipeline [this])
  (set-replica! [this new-replica])
  (set-exhausted! [this exhausted?])
  (exhausted? [this])
  (get-replica [this])
  (get-windows-state [this])
  (set-windows-state! [this new-windows-state])
  (add-barrier! [this epoch barrier])
  (remove-barrier! [this epoch])
  (get-lifecycle [this])
  (print-state [this])
  (get-barrier [this epoch])
  (set-event! [this new-event])
  (get-event [this])
  (set-messenger! [this new-messenger])
  (get-messenger [this])
  (set-coordinator! [this new-coordinator])
  (get-coordinator [this])
  (set-context! [this new-context])
  (get-context [this])
  (start-recover! [this])
  (exec [this])
  (advance [this]))
