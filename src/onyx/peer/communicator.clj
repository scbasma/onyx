(ns onyx.peer.communicator
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll! put!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal trace]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [onyx.peer.log-version]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clojure.string :as str]))

(defn outbox-loop [log outbox-ch group-ch]
  (loop []
    (when-let [entry (<!! outbox-ch)]
      (try
       (trace "Log Writer: wrote - " entry)
       (let [log-entry-info (extensions/write-log-entry log entry)
             log-entry {:log-info log-entry-info :log-entry entry}]
          (put! group-ch [:epidemic-log-event log-entry]))
       (catch Throwable e
         (warn e "Replica services couldn't write to ZooKeeper.")
         (>!! group-ch [:restart-peer-group])))
      (recur))))

(defn close-outbox! [_ outbox-ch outbox-loop-thread]
  (close! outbox-ch)
  ;; Wait for outbox to drain in outbox-loop
  (<!! outbox-loop-thread))

(defrecord LogWriter [peer-config group-ch]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Log Writer")
    ;; Race to write the job scheduler and messaging to durable storage so that
    ;; non-peers subscribers can discover which properties to use.
    ;; Only one writer will succeed, and only one needs to.
    (let [log-parameters {:job-scheduler (:onyx.peer/job-scheduler peer-config)
                          :messaging (select-keys peer-config [:onyx.messaging/impl])
                          :log-version onyx.peer.log-version/version}]
      (extensions/write-chunk log :log-parameters log-parameters nil)
      (let [read-log-parameters (extensions/read-chunk log :log-parameters nil)]
        (onyx.peer.log-version/check-compatible-log-versions! (:log-version read-log-parameters))
        (when-not (= (dissoc log-parameters :log-version) 
                     (dissoc read-log-parameters :log-version))
          (throw (ex-info "Log parameters read from cluster differ from the local parameters."
                          {:log-parameters log-parameters 
                           :read-log-parameters read-log-parameters})))))
    (let [outbox-ch (chan (arg-or-default :onyx.peer/outbox-capacity peer-config))
          outbox-loop-thread (thread (outbox-loop log outbox-ch group-ch))]
      (assoc component
             :outbox-ch outbox-ch
             :outbox-loop-thread outbox-loop-thread)))

  (stop [{:keys [log outbox-loop-thread outbox-ch] :as component}]
    (taoensso.timbre/info "Stopping Log Writer")
    (close-outbox! log outbox-ch outbox-loop-thread)
    component))

(defn log-writer [peer-config group-ch]
  (->LogWriter peer-config group-ch))

(defrecord ReplicaSubscription [peer-config]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Replica Subscription")
    (let [group-id (random-uuid)
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

; the record responsible for actually applying the received log-entries
; keeps track of latest log-position and the current epidemic-record which is closest to the position number
; in the log.
(defn parse-entry [entry]
  (Integer/parseInt (last (str/split (str/trim (:log-info (:data entry))) #"-"))))

(def counter (atom -1))

(defn next-value []
  (swap! counter inc))

(defn epidemic-listening-loop [incoming-ch log-entries]
  (loop []
    (when-let [log-entry (<!! incoming-ch)]
      (println (str "EPIDEMIC ENTRIES RECEIVED: " (next-value) (sort-by parse-entry (conj log-entries log-entry)))))
    (recur)))

(defrecord EpidemicApplier [peer-config]
  component/Lifecycle

  (start [{:keys [epidemic-messenger] :as component}]
    (let [incoming-ch (:incoming-ch epidemic-messenger)
          log-entries #{}
          epidemic-listening-thread (thread (epidemic-listening-loop incoming-ch log-entries))
          ]
      (assoc component
        :incoming-ch incoming-ch)))
  (stop [component]
    (assoc component
      :incoming-ch nil)))

(defn epidemic-applier [peer-config]
  (->EpidemicApplier peer-config))

(defrecord OnyxComm []
  component/Lifecycle
  (start [this]
    (component/start-system this [:log :logging-config :replica-subscription :log-writer :epidemic-applier]))
  (stop [this]
    (component/stop-system this [:log :logging-config :replica-subscription :log-writer :epidemic-applier])))


(defn onyx-comm
  [peer-config group-ch monitoring epidemic-messenger]
   (map->OnyxComm
    {:config peer-config
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring monitoring
     :log (component/using (zookeeper peer-config) [:monitoring])
     :replica-subscription (component/using (replica-subscription peer-config) [:log])
     :log-writer (component/using (log-writer peer-config group-ch) [:log])
     :epidemic-messenger epidemic-messenger
     :epidemic-applier (component/using (epidemic-applier peer-config) [:epidemic-messenger])}))
