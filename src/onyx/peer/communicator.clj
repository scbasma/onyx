(ns onyx.peer.communicator
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll! put! mult tap untap]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal trace]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [onyx.peer.log-version]
            [onyx.static.default-vals :refer [arg-or-default]]
            [clojure.string :as str]
            [onyx.messaging.protocols.epidemic-messenger :as epm]
            [onyx.messaging.aeron.epidemic-messenger :refer [parse-entry]]))

(defn outbox-loop [log outbox-ch group-ch]
  (loop []
    (when-let [entry (<!! outbox-ch)]
      (try
       (trace "Log Writer: wrote - " entry)
       (let [log-entry-info (extensions/write-log-entry log entry)
             log-entry (merge {:log-info log-entry-info} entry)]
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


(defn next-entry? [entry-state entry]
  (if (some? entry)
    (= (:message-id entry) (inc (:pos entry-state)))))

(defn clean-entry [entry]
  (reduce #(dissoc %1 %2 ) entry [:TTL :transmitter-list :log-info]))

(defn add-entry-to-state [entry-state new-entry]
   (if (nil? (:pos entry-state))
    (assoc entry-state :pos (:message-id new-entry))
    (if (some? new-entry)
      (let [entry-pos (:message-id new-entry)
            pos (:pos entry-state)
            entries (:entries entry-state)]

        (if (< entry-pos pos)
          entry-state
          (if (next-entry? entry-state new-entry)
            (-> entry-state
                (assoc :pos entry-pos)
                (assoc :entries (distinct (filter #(> (:message-id %) entry-pos)
                                           (sort-by :message-id entries)))))
            (assoc entry-state :entries (sort-by :message-id (conj entries new-entry))))))
      entry-state)))


(defn log-entry-listening-loop [epidemic-inbox-ch log-inbox-ch inbox-ch]
  (loop [entry-state {:pos nil :entries []}]
    (let [chs [log-inbox-ch epidemic-inbox-ch]
          [dirty-log-entry ch] (alts!! chs :priority true)
          log-entry (clean-entry dirty-log-entry)]
      (println "ENTRY-STATE: " entry-state)
      (println "LOG-ENTRY: " log-entry)
      (if (some? log-entry)
        (if (nil? (:pos entry-state ))
          (if (= ch log-inbox-ch)
            (do
                (>!! inbox-ch log-entry)
              (recur (add-entry-to-state entry-state log-entry)))
            (recur entry-state))
          (let [new-entry-state (loop [state entry-state entry log-entry]
                                   (let [old-pos (:pos state)
                                         new-state (add-entry-to-state state entry)
                                         new-pos (:pos new-state)]
                                     (if (not= old-pos new-pos)
                                       (do
                                         (if (:log-entry entry)
                                           (>!! inbox-ch (:log-entry entry))
                                           (>!! inbox-ch entry)
                                           )
                                         (recur new-state (first (:entries new-state))))
                                       new-state)))]
            (recur new-entry-state)))))))

      ;(if (some? log-entry)
      ;  (let [entries (:entries new-entry-state)]
      ;    (if (nil? (:pos entry-state)) ;if pos is nil, this is first log-entry and should be collected from the log.
      ;      (>!! inbox-ch log-entry)
      ;     (if (= (:pos new-entry-state) (inc (:pos entry-state)) ) ;if pos is not nil and log-entry next in line, write to inbox-ch
      ;       (>!! inbox-ch log-entry)
      ;       (if (next-entry? new-entry-state (first entries))
      ;           (>!! inbox-ch (first entries)))))
      ;     ;if log-entry is not nil and not next in line, it means it is ahead of the replica, should check here too see if any other entry is applicable
      ;    (recur (if (next-entry? new-entry-state (first entries))
      ;           (assoc new-entry-state :pos (:message-id log-entry))
      ;           new-entry-state)))))))

(defrecord ReplicaSubscription [peer-config]
  component/Lifecycle

  (start [{:keys [log epidemic-subscription] :as component}]
    (taoensso.timbre/info "Starting Replica Subscription")
    (let [group-id (random-uuid)
          log-inbox-ch (chan (arg-or-default :onyx.peer/inbox-capacity peer-config))
          epidemic-inbox-ch (:incoming-ch epidemic-subscription)
          inbox-ch (chan (arg-or-default :onyx.peer/inbox-capacity peer-config))
          origin (extensions/subscribe-to-log log log-inbox-ch)
          event-listening-thread (thread (log-entry-listening-loop epidemic-inbox-ch log-inbox-ch inbox-ch))
          ]
      (assoc component
             :group-id group-id
             :log-inbox-ch log-inbox-ch
             :inbox-ch inbox-ch
             :replica-origin origin)))

  (stop [component]
    (taoensso.timbre/info "Stopping Replica Subscription")
    (close! (:log-inbox-ch component))
    (close! (:inbox-ch component))
    (<!! (:event-listening-thread component))
    component))

(defn replica-subscription [peer-config]
  (->ReplicaSubscription peer-config))

; the record responsible for actually applying the received log-entries
; Should keep track of latest log-position and should receive the log-entry which is closest to the position number
; in the log from the epidemic-messenger.


(defrecord EpidemicApplier [peer-config]
  component/Lifecycle
  (start [{:keys [epidemic-messenger replica-subscription] :as component}]
    (let [incoming-ch (:incoming-ch epidemic-messenger)
          mult-inbox (:mult-inbox-ch replica-subscription)
          inbox-ch (tap mult-inbox (chan (arg-or-default :onyx.peer/inbox-capacity peer-config)))]
      (assoc component
        :mult-inbox mult-inbox
        :incoming-ch incoming-ch
        :inbox-ch inbox-ch)))
  (stop [component]
    (close! (:incoming-ch component))
    (untap (:mult-inbox component) (:inbox-ch component))
    (close! (:inbox-ch component))
    (assoc component
      :incoming-ch nil
      :mult-inbox nil
      :tapped-inbox-ch nil)))

(defn epidemic-applier [peer-config]
  (->EpidemicApplier peer-config))

(defrecord OnyxComm []
  component/Lifecycle
  (start [this]
    (component/start-system this [:log :logging-config :replica-subscription :log-writer]))
  (stop [this]
    (component/stop-system this [:log :logging-config :replica-subscription :log-writer])))


(defn onyx-comm [peer-config group-ch monitoring epidemic-messenger]
   (map->OnyxComm
    {:config peer-config
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring monitoring
     :log (component/using (zookeeper peer-config) [:monitoring])
     :log-writer (component/using (log-writer peer-config group-ch) [:log])
     :epidemic-subscription epidemic-messenger
     :replica-subscription (component/using (replica-subscription peer-config) [:log :epidemic-subscription])
     }))
