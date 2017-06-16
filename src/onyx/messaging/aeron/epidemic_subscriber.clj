(ns onyx.messaging.aeron.epidemic-subscriber
  (:require [onyx.messaging.protocols.epidemic-subscriber :as esub]
            [onyx.messaging.aeron.subscriber :refer [available-image unavailable-image new-error-handler]]
            [onyx.messaging.aeron.utils :as autil]
            [taoensso.timbre :refer [info debug warn] :as timbre]
            [onyx.compression.nippy :refer [messaging-decompress]]
            [onyx.messaging.serialize :as sz]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.messaging.aeron.utils :refer [try-close-conn try-close-subscription]]
            [clojure.core.async :refer [>!!]])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy]
           [org.agrona ErrorHandler]
           [onyx.messaging.aeron.int2objectmap CljInt2ObjectHashMap]
           [io.aeron Aeron Aeron$Context Publication Subscription Image
                     ControlledFragmentAssembler UnavailableImageHandler
                     AvailableImageHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action FragmentHandler]
           (java.util.function Consumer)))


(defn dummy-deserialize [^UnsafeBuffer buf offset length]
  (let [bs (byte-array length)]
    (.getBytes buf offset bs)
    (messaging-decompress bs)))

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

(deftype EpidemicSubscriber [peer-id peer-config site batch-size incoming-ch ^AtomicLong read-bytes
                             ^AtomicLong error-counter error ^bytes bs channel ^Aeron conn
                             ^Subscription subscription lost-sessions
                             ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler
                             ^:unsynchronized-mutable stream-id
                             ^:unsynchronized-mutable publication
                             ^:unsynchronized-mutable batch]

  esub/EpidemicSubscriber
  (start [this]
    (let [
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          lost-sessions (atom #{})
          sinfo [peer-id site]
          ctx (cond-> (.errorHandler (Aeron$Context.)
                                     (new-error-handler error error-counter))
                      media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          available-image-handler (available-image sinfo lost-sessions)
          unavailable-image-handler (unavailable-image sinfo)
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          stream-id 1001
          sub (.addSubscription conn channel stream-id available-image-handler unavailable-image-handler)
          sources []
          status-pubs {}
          status {}
          new-subscriber (esub/add-assembler
                           (EpidemicSubscriber. peer-id peer-config site batch-size incoming-ch read-bytes
                                                error-counter error bs channel conn sub lost-sessions
                                                nil stream-id nil nil))
          shutdown (atom false)
          listening-thread (start-subscriber! new-subscriber shutdown peer-config)]
    ;(info "Created subscriber" (esub/info new-subscriber))
     new-subscriber))
  (stop [this]
    (some-> subscription try-close-subscription)
    (some-> conn try-close-conn)
    (EpidemicSubscriber. peer-id peer-config site batch-size incoming-ch (AtomicLong.)
                        (AtomicLong. ) (atom nil) (byte-array (autil/max-message-length))
                        nil nil nil nil nil nil nil nil))

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
    (let [message (dummy-deserialize buffer (inc offset) (dec length))]
      (println "putting log-event message: " message)
      (>!! incoming-ch message))))


(defn new-epidemic-subscriber [peer-config monitoring peer-id
                               {:keys [site batch-size] :as sub-info} incoming-ch]
  (->EpidemicSubscriber peer-id peer-config site batch-size incoming-ch (AtomicLong.)
                        (AtomicLong. ) (atom nil) (byte-array (autil/max-message-length))
                        nil nil nil nil nil nil nil nil))
