(ns onyx.messaging.aeron.epidemic-subscriber
  (:require [onyx.messaging.protocols.epidemic-subscriber :as esub]
            [onyx.messaging.aeron.subscriber :refer [available-image unavailable-image]]
            [onyx.messaging.aeron.utils :as autil]
            [taoensso.timbre :refer [info debug warn] :as timbre]
            [onyx.messaging.serialize :as sz]
            [onyx.static.default-vals :refer [arg-or-default]])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy]
           [org.agrona ErrorHandler]
           [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]
           [onyx.messaging.aeron.int2objectmap CljInt2ObjectHashMap]
           [io.aeron Aeron Aeron$Context Publication Subscription Image
                     ControlledFragmentAssembler UnavailableImageHandler
                     AvailableImageHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action FragmentHandler]
           (java.util.function Consumer)))

(def fragment-limit-receiver 100000)

(defn consumer [subscription idle-strategy shutdown failed]
  (reify Consumer
    (accept [this subscription]
      (while (and (not @shutdown) (not @failed))
        (let [fragments-read (esub/poll! subscription)]
          (.idle idle-strategy fragments-read))))))

(defn start-subscriber! [subscription shutdown peer-config]
  (future
    (loop []
      (let [failed (atom false)
            idle-strategy (BackoffIdleStrategy. 5
                                                5
                                                (arg-or-default :onyx.peer/idle-min-sleep-ns peer-config)
                                                (arg-or-default :onyx.peer/idle-max-sleep-ns peer-config))]
          (try (.accept ^Consumer (consumer subscription idle-strategy shutdown failed) subscription)
             (catch Throwable e
               (reset! failed true)
               (warn "Subscriber failed: " e))))
      (when-not @shutdown
        (info "Subscriber failed.")
        (recur)))
    (info "Shutting down subscriber.")))

(deftype EpidemicSubscriber [peer-id peer-config site batch-size ^AtomicLong read-bytes
                             ^AtomicLong errors error ^bytes bs channel ^Aeron conn
                             ^Subscription subscription lost-sessions
                             ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler
                             ^:unsynchronized-mutable stream-id
                             ^:unsynchronized-mutable publication
                             ^:unsynchronized-mutable batch]

  esub/EpidemicSubscriber
  (start [this]
    (let [error-handler (reify ErrorHandler
                         (onError [this x]
                           (reset! error x)
                           (.addAndGet errors 1)))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          lost-sessions (atom #{})
          sinfo [peer-id site]
          ctx (cond-> (Aeron$Context.)
                      error-handler (.errorHandler error-handler)
                      media-driver-dir (.aeronDirectoryName ^String media-driver-dir)
                      true (.availableImageHandler (available-image sinfo))
                      true (.unavailableImageHandler (unavailable-image sinfo lost-sessions)))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          stream-id 1001
          sub (.addSubscription conn channel stream-id)
          sources []
          status-pubs {}
          status {}
          new-subscriber (esub/add-assembler
                           (EpidemicSubscriber. peer-id peer-config site batch-size read-bytes
                                                errors error bs channel conn sub lost-sessions
                                                nil stream-id nil nil))
          shutdown (atom false)
          listening-thread (start-subscriber! new-subscriber shutdown peer-config)]
    ;(info "Created subscriber" (esub/info new-subscriber))
     new-subscriber))
  (stop [this])
  (add-assembler [this]
    (set! assembler (ControlledFragmentAssembler. this))
    this)
  (update-stream-id [sub new-stream-id]
    (set! stream-id new-stream-id)
    sub)
  (poll! [this]
    (when @error (throw @error))
    (set! batch nil)
    (->> (.controlledPoll ^Subscription subscription
                          ^ControlledFragmentHandler assembler
                          fragment-limit-receiver)
         (.addAndGet read-bytes))
    batch)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [message (sz/deserialize buffer (inc offset) (dec length))]
      (println "received log-event message: " message))))

(defn new-epidemic-subscriber [peer-config monitoring peer-id
                               {:keys [site batch-size] :as sub-info}]
  (->EpidemicSubscriber peer-id peer-config site batch-size (:read-bytes monitoring)
                        (:subscription-errors monitoring) (atom nil) (byte-array (autil/max-message-length))
                        nil nil nil nil nil nil nil nil))
