(ns onyx.messaging.aeron.epidemic-publisher
  (:require [onyx.messaging.protocols.epidemic-publisher :as epub]
            [onyx.messaging.aeron.utils :as autil]
            [taoensso.timbre :refer [debug info warn] :as timbre]
            [onyx.messaging.serialize :as sz]
            [onyx.compression.nippy :refer [messaging-compress]])
  (:import [io.aeron Aeron Aeron$Context Publication UnavailableImageHandler AvailableImageHandler]
           [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]))

(defn put-message-type [^UnsafeBuffer buf offset type-id]
  (.putByte buf offset type-id))

(defn dummy-serialize ^UnsafeBuffer [msg]
  (let [bs ^bytes (messaging-compress msg)
        length (inc (alength bs))
        buf (UnsafeBuffer. (byte-array length))]
    (put-message-type buf 0 (:type msg))
    (.putBytes buf 1 bs)
    buf))

(deftype EpidemicPublisher [peer-config src-peer-id site ^AtomicLong written-bytes
                            ^AtomicLong errors ^Aeron conn ^Publication publication error
                            ^:unsynchronized-mutable stream-id]
  epub/EpidemicPublisher
  (start [this]
    ;(println "Starting Epidemic Publisher")
    (let [error-handler (reify ErrorHandler
                          (onError [this x]
                            (println (str "ERROR: " x))
                            (.addAndGet errors 1)
                            (reset! error x)))
          media-driver-dir (:onyx-messaging.aeron/media-driver-dir peer-config)
          ctx (cond-> (Aeron$Context.)
                      error-handler (.errorHandler error-handler)
                      media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          stream-id 1001
          conn (Aeron/connect ctx)
          channel (autil/channel (:address site) (:port site))
          pub (.addPublication conn channel stream-id)]
      (EpidemicPublisher. peer-config src-peer-id site written-bytes errors conn pub error stream-id)))

  (stop [this]
    (info "Stopping Epidemic Publisher")
    (some-> publication autil/try-close-publication)
    (some-> conn autil/try-close-conn)
    (EpidemicPublisher. peer-config src-peer-id site written-bytes errors nil nil error nil))

  (update-stream-id [pub new-stream-id]
    (set! stream-id new-stream-id)
    pub)
  (offer-log-event! [this log-event]
    (let [msg (assoc log-event :type 101)
          ;_ (println "offering log event")
          buf (dummy-serialize msg)
          ;_ (println "after buf creation")
          ret (.offer ^Publication publication buf 0 (.capacity buf))])))
      ;(println (str "Offered log event from epidemic publisher, ret: " ret))





(defn new-epidemic-publisher [peer-config monitoring {:keys [src-peer-id site] :as pub-info}]
  (->EpidemicPublisher peer-config src-peer-id site (:written-bytes monitoring)
                       (:publication-errors monitoring) nil nil (atom nil) nil))
