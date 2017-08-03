(ns onyx.messaging.aeron.epidemic-messenger
  (:require [onyx.messaging.protocols.epidemic-messenger :as emp]
            [onyx.messaging.protocols.epidemic-subscriber :as esub]
            [onyx.messaging.aeron.epidemic-subscriber :refer [new-epidemic-subscriber]]
            [onyx.messaging.protocols.epidemic-publisher :as epub]
            [onyx.messaging.aeron.epidemic-publisher :refer [new-epidemic-publisher]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [debug info warn]]
            [onyx.static.uuid :refer [random-uuid]]
            [clojure.core.async :refer [chan close! >!! <!!]]
            [clojure.string :as str]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.messaging.protocols.epidemic-messenger :as epm])
  (:import (org.agrona.concurrent BackoffIdleStrategy IdleStrategy)
           (java.util.function Consumer)))

(defn stream-pool [peer-count])

(defn parse-entry [entry]
  ;(println "PARSE-ENTRY: " entry)
  (if (:log-info entry)
    (Integer/parseInt (last (str/split (str/trim (:log-info entry)) #"-")))
    0))


(deftype AeronEpidemicMessenger [peer-config messenger-group monitoring incoming-ch messenger-id
                                 ^:unsynchronized-mutable log-entries
                                 ^:unsynchronized-mutable subscriber
                                 ^:unsynchronized-mutable publisher]

  emp/EpidemicMessenger
  (start [this]
    (info "Starting Aeron Epidemic Messenger...")
      (assert incoming-ch)
      (-> (AeronEpidemicMessenger. peer-config messenger-group monitoring incoming-ch messenger-id #{} nil nil)
          (emp/update-publisher {:stream-id 1001 :site {:address "localhost" :port 40199} :peer-id 1111})
          (emp/update-subscriber {:stream-id 1001 :site {:address "localhost" :port 40199} :peer-id 1111} incoming-ch) ))


  (stop [messenger]
    (info "Stopping Aeron Epidemic Messenger...")
    (when subscriber (esub/stop subscriber))
    (when publisher (epub/stop publisher))
    (set! subscriber nil)
    (set! publisher nil)
    messenger)

  (info [this])

  (update-subscriber [messenger sub-info incoming-ch]
    (assert sub-info)
    (set! subscriber
          (esub/update-stream-id
            (or subscriber (esub/start (new-epidemic-subscriber
                                         messenger peer-config monitoring 121 sub-info incoming-ch)))
            (:stream-id sub-info)))
    messenger)
  (update-publisher [messenger pub-info]
    (assert pub-info)
    (set! publisher
          (epub/update-stream-id
            (or publisher (epub/start (new-epidemic-publisher
                                        peer-config monitoring pub-info)))
            (:stream-id pub-info)))
    messenger)

  (initiate-log-entries [messenger initial-log-entries]
    (set! log-entries initial-log-entries))

  (update-log-entries [messenger log-event]
    ;(println (str "inside update-log-entries with log-event: " log-event " and log-entries: " log-entries))
    (when (nil? log-entries) (set! log-entries #{}))
    (let [earlier-first (parse-entry (first log-entries))
          new-log-entries (set! log-entries
                            (reverse (sort-by parse-entry
                                              (conj log-entries log-event))))
          ;_ (println (str "log entries in update-log-entries: " log-entries "with messenger id: " messenger-id))
          new-first (parse-entry (first new-log-entries))]
      (when (or (> new-first earlier-first) (= (count log-entries) 1))
        (>!! incoming-ch (if (:message-id (first log-entries))
                           (first log-entries)
                           (assoc (first log-entries) :message-id (parse-entry (first log-entries)))))
        ))

    ;(println (str "Received log-event: " log-event "\n\t in messenger: " messenger-id "\n\t and log-entries: " log-entries))
    (if-let [transmitter-list (:transmitter-list log-event)]
      (if (not (some #(= messenger-id %) (:transmitter-list log-event)))
        (emp/offer-log-event! messenger
                              (assoc log-event :transmitter-list (conj (:transmitter-list log-event) messenger-id))))
      (do
        ;(println "LOG-EVENT: " log-event)
        (emp/offer-log-event! messenger (->  log-event
                                            (assoc :transmitter-list (conj [] messenger-id))
                                            (assoc :message-id (parse-entry log-event))
                                           ))))
    messenger)
  (get-latest-log-event [messenger]
    (first log-entries))
  (get-all-log-events [messenger]
    (. messenger log-entries))
  (get-messenger-id [messenger]
    messenger-id)
  (offer-log-event! [messenger log-event]
    ;(println "offering log event: " log-event)
    (assert publisher)
    (if-let [TTL (:TTL log-event)]
      (if (not (zero? (dec (:TTL log-event))))
        (epub/offer-log-event! publisher (assoc log-event :TTL (dec TTL))))
      (epub/offer-log-event! publisher (assoc log-event :TTL 2))))

  (subscriber [messenger]
    subscriber))


(defn build-aeron-epidemic-messenger [peer-config messenger-group monitoring incoming-ch]
  (emp/start (->AeronEpidemicMessenger peer-config messenger-group monitoring incoming-ch (random-uuid) nil nil nil)))

(defrecord EpidemicMessenger [peer-config messenger-group monitoring aeron-epidemic-messenger]
  component/Lifecycle
  (start [{:keys [messenger-group monitoring] :as component}]
    (let [incoming-ch (chan 1000)
          aeron-epidemic-messenger (build-aeron-epidemic-messenger peer-config messenger-group monitoring incoming-ch)]
      (assoc component
        :messenger-group messenger-group
        :monitoring monitoring
        :aeron-epidemic-messenger aeron-epidemic-messenger
        :incoming-ch incoming-ch)))
  (stop [{:keys [messenger-group monitoring aeron-epidemic-messenger incoming-ch] :as component}]
    (when aeron-epidemic-messenger (emp/stop aeron-epidemic-messenger))
    (close! incoming-ch)
    (assoc component
      :messenger-group nil
      :monitoring nil
      :aeron-epidemic-messenger nil
      :incoming-ch nil)))

(defn build-epidemic-messenger [peer-config]
  (->EpidemicMessenger peer-config nil nil nil))
