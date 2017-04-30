(ns onyx.peer.communicator
  (:require [clojure.core.async :refer [>! >!! <!! alts!! promise-chan close! chan thread poll! put!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal trace]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.extensions :as extensions]
            [onyx.peer.log-version]
            [onyx.static.default-vals :refer [arg-or-default]]))

(defn outbox-loop [log outbox-ch group-ch]
  (loop []
    (when-let [entry (<!! outbox-ch)]
      (try
       (trace "Log Writer: wrote - " entry)
       (extensions/write-log-entry log entry)
       (put! group-ch [:epidemic-log-event entry])
       (catch Throwable e
         (warn e "Replica services couldn't write to ZooKeeper.")
         (>!! group-ch [:restart-peer-group])))
      (recur))))

(defrecord LogWriter [peer-config group-ch]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Log Writer")
    ;; Race to write the job scheduler and messaging to durable storage so that
    ;; non-peers subscribers can discover which properties to use.
    ;; Only one writer will succeed, and only one needs to.

    (extensions/write-chunk log
                            :log-parameters
                            {:job-scheduler (:onyx.peer/job-scheduler peer-config)
                             :messaging (select-keys peer-config [:onyx.messaging/impl])
                             :log-version onyx.peer.log-version/version}
                            nil)
    (let [outbox-ch (chan (arg-or-default :onyx.peer/outbox-capacity peer-config))
          outbox-loop-thread (thread (outbox-loop log outbox-ch group-ch))]
      (assoc component
             :outbox-ch outbox-ch
             :outbox-loop-thread outbox-loop-thread)))

  (stop [{:keys [outbox-loop-thread outbox-ch] :as component}]
    (taoensso.timbre/info "Stopping Log Writer")
    (close! outbox-ch)
    ;; Wait for outbox to drain
    (<!! outbox-loop-thread)
    component))

(defn log-writer [peer-config group-ch]
  (->LogWriter peer-config group-ch))

(defrecord ReplicaSubscription [peer-config]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Replica Subscription")
    (let [group-id (java.util.UUID/randomUUID)
          inbox-ch (chan (arg-or-default :onyx.peer/inbox-capacity peer-config))
          origin (extensions/subscribe-to-log log inbox-ch)]
      (assoc component
             :group-id group-id
             :inbox-ch inbox-ch
             :replica-origin origin)))

  (stop [component]
    (taoensso.timbre/info "Stopping Replica Subscription")
    (close! (:inbox-ch component))
    component))

(defn replica-subscription [peer-config]
  (->ReplicaSubscription peer-config))

(defrecord OnyxComm []
  component/Lifecycle
  (start [this]
    (component/start-system this [:log :logging-config :replica-subscription :log-writer]))
  (stop [this]
    (component/stop-system this [:log :logging-config :replica-subscription :log-writer])))

(defn onyx-comm
  [peer-config group-ch monitoring]
  (map->OnyxComm
   {:config peer-config
    :logging-config (logging-config/logging-configuration peer-config)
    :monitoring monitoring
    :log (component/using (zookeeper peer-config) [:monitoring])
    :replica-subscription (component/using (replica-subscription peer-config) [:log])
    :log-writer (component/using (log-writer peer-config group-ch) [:log])}))
