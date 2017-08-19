(ns onyx.messaging.aeron.epidemic-subscriber
  (:require [onyx.messaging.protocols.epidemic-subscriber :as esub]
            [onyx.messaging.aeron.subscriber :refer [available-image unavailable-image new-error-handler]]
            [onyx.messaging.aeron.utils :as autil]
            [taoensso.timbre :refer [info debug warn] :as timbre]
            [onyx.compression.nippy :refer [messaging-decompress]]
            [onyx.messaging.serialize :as sz]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.messaging.aeron.utils :refer [try-close-conn try-close-subscription]]
            [clojure.core.async :refer [>!!]]
            [onyx.messaging.protocols.epidemic-messenger :as epm])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent AtomicBuffer IdleStrategy BackoffIdleStrategy]
           [org.agrona ErrorHandler]
           [onyx.messaging.aeron.int2objectmap CljInt2ObjectHashMap]
           [io.aeron Aeron Aeron$Context Publication Subscription Image
                     ControlledFragmentAssembler UnavailableImageHandler
                     AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action FragmentHandler]
           (java.util.function Consumer)))



(defn dummy-deserialize [^AtomicBuffer buf offset length]
  (let [bs (byte-array length)]
    (.getBytes buf offset bs)
    (messaging-decompress bs)))

(def fragment-limit-receiver 100000)

(defn consumer [subscription ^IdleStrategy idle-strategy shutdown failed]
  (reify Consumer
    (accept [this subscription]
      (while (and (not @shutdown) (not @failed))
        (let [fragments-read (esub/poll! subscription)]
          (if fragments-read (.idle idle-strategy fragments-read)))))))


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
               (println (str "Failed subscriber: " (.printStackTrace e)))
               (warn "Subscriber failed: " e))
               (finally
                 (try-close-subscription subscription))))
      (when-not @shutdown
        (info "Subscriber failed.")
        (recur)))
    (info "Shutting down subscriber.")))

(deftype EpidemicSubscriber [messenger peer-id peer-config site batch-size incoming-ch ^AtomicLong read-bytes
                             ^AtomicLong error-counter error ^bytes bs channel ^Aeron conn
                             ^Subscription subscription lost-sessions
                             ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler
                             ^:unsynchronized-mutable stream-id
                             ^:unsynchronized-mutable publication
                             ^:unsynchronized-mutable batch
                             shutdown]

  esub/EpidemicSubscriber
  (start [this]
    (info "Starting Epidemic Subscriber")
    (let [
          _ (println "MESSENGER ID IN SUBSCRIBER: " (epm/get-messenger-id messenger))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          lost-sessions (atom #{})
          sinfo [peer-id site]
          ctx (cond-> (.errorHandler (Aeron$Context.)
                                     (new-error-handler error error-counter))
                      media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          available-image-handler (available-image sinfo lost-sessions)
          unavailable-image-handler (unavailable-image sinfo)
          conn (Aeron/connect ctx)
          _ (println "PEER CONFIG IN SUB: " peer-config)
          _ (println "STREAM ID IN SUB: " stream-id)

         channel (autil/channel (:address site) 40199)



          sub (.addSubscription conn channel stream-id available-image-handler unavailable-image-handler)
          shutdown (atom false)
          new-subscriber (esub/add-assembler
                           (EpidemicSubscriber.  messenger peer-id peer-config site batch-size incoming-ch read-bytes
                                                error-counter error bs channel conn sub lost-sessions
                                                nil stream-id nil nil shutdown))
          listening-thread (start-subscriber! new-subscriber shutdown peer-config)]

    ;(info "Created subscriber" (esub/info new-subscriber))
     new-subscriber))
  (stop [this]
    (info "Stopping Epidemic Subscriber")
    (reset! shutdown true)
    (some-> subscription try-close-subscription)
    (some-> conn try-close-conn)
    (EpidemicSubscriber. messenger peer-id peer-config site batch-size incoming-ch (AtomicLong.)
                        (AtomicLong. ) (atom nil) (byte-array (autil/max-message-length))
                        nil nil nil nil nil nil nil nil nil))

  (add-assembler [this]
    (set! assembler (FragmentAssembler. this))
    this)
  (update-stream-id [sub new-stream-id]
    (set! stream-id new-stream-id)
    sub)
  (poll! [this]
    (when @error (throw @error))
    (.poll ^Subscription subscription
                          ^FragmentHandler assembler
                          10))
  FragmentHandler
  (onFragment [this buffer offset length header]
    ;(println (str "LENGTH IN ONFRAGMENT: " length))
    ;(println (str "CAPACITY OF BUFFER: " (.capacity buffer)))
    (let [message (dummy-deserialize buffer (inc offset) (dec length))]
        (epm/update-log-entries messenger message))))

(defn new-epidemic-subscriber [messenger peer-config monitoring peer-id
                               {:keys [site batch-size] :as sub-info} incoming-ch]
  (->EpidemicSubscriber messenger peer-id peer-config site batch-size incoming-ch (AtomicLong.)
                        (AtomicLong. ) (atom nil) (byte-array (autil/max-message-length))
                        nil nil nil nil nil (:stream-id sub-info) nil nil nil))
