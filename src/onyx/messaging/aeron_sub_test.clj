(ns ^:no-doc onyx.messaging.aeron-sub-test
  (:require [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.compression.nippy :refer [window-log-decompress window-log-compress]])
  (:import [io.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

;comment
(defn delete-aeron-directory-safe [^MediaDriver$Context media-driver-context]
  (try (.deleteAeronDirectory media-driver-context)
       (catch java.nio.file.NoSuchFileException nsfe
         (info "Couldn't delete aeron media dir. May have been already deleted by shutdown hook." nsfe))))

(deftype FragHandler [f]
  FragmentHandler
  (onFragment [this buffer offset length header]
    (f buffer offset length header)))

(defn handle-message [{:keys [read poll-iteration] :as sub} buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (window-log-decompress ba)]
    (swap! read conj [(:id message) (.position header) @poll-iteration])))

(defn new-subscription [stream-id]
  (let [error-handler (reify ErrorHandler
                        (onError [this x] 
                          (println "Aeron messaging subscriber error" x)))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler)
                ;(.availableImageHandler (available-image sub-info))
                ;(.unavailableImageHandler (unavailable-image-drainer sub-info))
                )
        conn (Aeron/connect ctx)
        bind-addr "127.0.0.1"
        port 42000
        channel (mc/aeron-channel bind-addr port)
        _ (println "Add subscription " channel stream-id conn)
        subscription (.addSubscription conn channel stream-id)
        sub {:subscription subscription
             :stream stream-id
             :conn conn
             :id (java.util.UUID/randomUUID)
             :poll-iteration (atom 0)
             :read (atom [])}
        handler (FragmentAssembler. 
                 (FragHandler. (fn [buffer offset length header]
                                 (println "Polled position" (.position header))
                                 (handle-message sub buffer offset length header))))]
    (assoc sub :handler handler)))

(defn close-sub! [sub-info]
  (println "Closing sub" (.images (:subscription sub-info)))
  (while (not (empty? (.images (:subscription sub-info))))
    (println "Burning while still images"))
  (.close ^Subscription (:subscription sub-info))
  (.close (:conn sub-info)))

(defn new-publication [stream-id]
  (let [address "127.0.0.1"
        port 42000
        channel (mc/aeron-channel address port)
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          (println "Aeron messaging publication error" x)))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        _ (println "Creating new pub" channel stream-id)
        pub (.addPublication conn channel stream-id)]
    {:conn conn :publication pub :stream stream-id :sent (atom [])}))

(defn close-pub! [pub-info]
  (.close ^Publication (:publication pub-info))
  (.close (:conn pub-info)))

(defn poll! [sub]
  (let [_ (swap! (:poll-iteration sub) inc)]
    (.poll ^Subscription (:subscription sub) ^FragmentHandler (:handler sub) 10)))

(defrecord EmbeddedMediaDriver []
  component/Lifecycle
  (start [component]
    (let [media-driver-context (-> (MediaDriver$Context.) 
                                     (.threadingMode ThreadingMode/SHARED)
                                     (.dirsDeleteOnStart true))
          media-driver (MediaDriver/launch media-driver-context)]
      (assoc component 
             :media-driver media-driver 
             :media-driver-context media-driver-context)))
  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (when media-driver 
      (.close ^MediaDriver media-driver))
    (when media-driver-context 
      (delete-aeron-directory-safe media-driver-context))
    (assoc component :media-driver nil :media-driver-context nil)))

(defn offer-message! [pub message]
  (let [payload ^bytes (window-log-compress message)
        buf ^UnsafeBuffer (UnsafeBuffer. payload)]
    (let [ret (.offer ^Publication (:publication pub) buf 0 (.capacity buf))]
      (when (pos? ret)
        (println "ret" ret (:id message))
        ;(println "size" (alength payload))
        ;; Offer successful, track what was sent
        (swap! (:sent pub) conj (:id message))))))

(defn try-run []
  (let [media-driver (component/start (->EmbeddedMediaDriver))]
    (try
     (dotimes [stream-id 1] 
       (let [n-subscribers 1
             n-publishers 1
             subs (atom (mapv (fn [i] (new-subscription stream-id))
                              (range n-subscribers)))
             pubs (mapv (fn [i] (new-publication stream-id))
                        (range n-publishers))]
         (Thread/sleep 2000)
         (try
          (dotimes [i 100]
            (when true ;(zero? (rand-int 10)) 
              (offer-message! (first pubs) {:id i :rand-str (apply str (repeatedly 200 #(rand-int 10000)))}))
            #_(dotimes [j 100] 
              (run! (fn [sub] 
                      (when true
                        (poll! sub))) 
                    @subs)))

          (println "Closed sub")
          (close-sub! (first @subs))
          (println "Reopening")

          (dotimes [i 100]
            (when true ;(zero? (rand-int 10)) 
              (offer-message! (first pubs) {:id i :rand-str (apply str (repeatedly 200 #(rand-int 10000)))}))
            (dotimes [j 100] 
              (run! (fn [sub] 
                      (when true
                        (poll! sub))) 
                    @subs))
            )

          #_(reset! subs 
                  (mapv (fn [i] (new-subscription stream-id))
                        (range (inc n-subscribers))))

          #_(dotimes [i 100]
              (when true ;(zero? (rand-int 10)) 
                (offer-message! (first pubs) {:id i :rand-str (apply str (repeatedly 200 #(rand-int 10000)))}))
            ;; sometimes poll
            #_(run! (fn [sub] 
                    (when true ;(zero? (rand-int 5)) 
                      (poll! sub))) 
                  subs))


          #_(dotimes [i 100000]
            (run! (fn [sub] 
                    (when (zero? (rand-int 50)) 
                      (poll! sub))) 
                  subs))

          #_(Thread/sleep 500)

          ;; Try to flush the rest
          #_(dotimes [i 10000000]
            (run! poll! subs))

          ;(Thread/sleep 2000)

          ;; Try to flush the rest again
          ; (dotimes [i 10000000]
          ;   (run! poll! subs))
          #_(let [sent-missing (mapv (fn [sub] (sort (vec (clojure.set/difference (set @(:sent (first pubs)))
                                                                                (set (map first @(:read sub)))))))

                                   subs)]
            (assert (empty? (reduce into [] sent-missing))
                    (mapv (fn [sub positions sent-missing]
                            [:positions positions :sent-missing sent-missing :read @(:read sub)])
                          subs
                          (mapv (fn [sub] (map (fn [i] (.position i)) (.images (:subscription sub))))
                                subs)     
                          sent-missing)))
          (finally 
           (run! close-pub! pubs)
           (run! close-sub! @subs)))))
     (finally
      (component/stop media-driver)))))

