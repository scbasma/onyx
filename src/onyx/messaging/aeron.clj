(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Message ->Barrier ->BarrierAck]]
            [onyx.messaging.messenger :as m]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [io.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(defn barrier? [v]
  (instance? onyx.types.Barrier v))

(defn message? [v]
  (instance? onyx.types.Message v))

(defn ack? [v]
  (instance? onyx.types.BarrierAck v))

(def tracked-messages (atom {}))
(def sent-barriers (atom []))

(defn reset-tracked-messages! [] (reset! tracked-messages {}))

(defn add-tracked-message! [messenger dst-task-id src-peer-id message poll-ret poll-type]
  (swap! tracked-messages 
         update-in 
         [(:id messenger)
          {:dst-task-id dst-task-id 
           :src-peer-id src-peer-id}] 
         (fn [msgs] (conj (vec msgs) [poll-type 
                                      (m/replica-version messenger) 
                                      (m/epoch messenger)
                                      message
                                      poll-ret]))))



(comment 

(filter (fn [v]
          (let [msg (nth v 3)]
            ;(println msg)
            (and (= (:replica-version msg) 105)
                   (barrier? msg)
                   (= (:epoch msg) 1))))

        (reduce into [] (vals (reduce merge (vals @tracked-messages)))))
 
(filter (fn [v]
          (let [msg (nth v 3)]
            ;(println msg)
            (= (:replica-version msg) 105)
            #_(and (= (:replica-version msg) 105)
                 (barrier? msg)
                 (= (:epoch msg) 1))))

        (reduce into [] (vals (get @tracked-messages #uuid "445fcd16-7611-6ae4-c0bf-6228a118fdd4")
                         
                         #_(reduce merge (vals @tracked-messages)))))

 (filter (fn [barrier]
           (= {:dst-task-id [#uuid "144910a6-3ccc-404d-89aa-543031d45e79" :inc], 
               :src-peer-id #uuid "2c16fb10-04a3-dc50-d673-7fb9e8ff8c01"
               ;#uuid "3c15d80c-63a6-c9a6-caf2-a77e5e1a221e"
               }
              (select-keys barrier [:src-peer-id :dst-task-id]))) 
         @sent-barriers)

 
 
 (map (fn [[sub-info messages]] 
       (->> messages
            (filter (fn [[_ _ _ message _]]
                      (and 
                       (= 120 (:replica-version message))
                       (barrier? message)
                       (= (select-keys sub-info [:src-peer-id :dst-task-id])
                          (select-keys message [:src-peer-id :dst-task-id])))))
            (first)
            (vector sub-info)))
     (get-in @tracked-messages [;#uuid "c3dfe037-d678-aedb-2dd2-1f4d66fe3acc"
                                #uuid "2c16fb10-04a3-dc50-d673-7fb9e8ff8c01"
                                 ;#uuid "d4c42c8b-e4c7-c4b3-a676-0565b80560a0"
                                ])))

;; FIXME to be tuned
(def fragment-limit-receiver 100)

(defn backoff-strategy [strategy]
  (case strategy
    :busy-spin (BusySpinIdleStrategy.)
    :low-restart-latency (BackoffIdleStrategy. 100
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 1000
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 10)
                                                (.toNanos TimeUnit/MICROSECONDS 1000))))


(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defn stream-id [job-id task-id slot-id site]
  (hash [job-id task-id slot-id site]))

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod m/get-peer-site :aeron
  [peer-config]
  (println "GET PEER SITE" (mc/external-addr peer-config))
  {:address (mc/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

(defn delete-aeron-directory-safe [^MediaDriver$Context media-driver-context]
  (try (.deleteAeronDirectory media-driver-context)
       (catch java.nio.file.NoSuchFileException nsfe
         (info "Couldn't delete aeron media dir. May have been already deleted by shutdown hook." nsfe))))

(defrecord EmbeddedMediaDriver [peer-config]
  component/Lifecycle
  (start [component]
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? peer-config)
          threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading peer-config))
          media-driver-context (if embedded-driver?
                                 (-> (MediaDriver$Context.) 
                                     (.threadingMode threading-mode)
                                     (.dirsDeleteOnStart true)))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))]
      (when embedded-driver? 
        (.addShutdownHook (Runtime/getRuntime) 
                          (Thread. (partial delete-aeron-directory-safe media-driver-context))))
      (assoc component 
             :media-driver media-driver 
             :media-driver-context media-driver-context)))
  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (when media-driver 
      (.close ^MediaDriver media-driver))
    (when media-driver-context 
      (delete-aeron-directory-safe media-driver-context))
    (assoc component :media-driver nil :media-driver-context nil)))

(defrecord AeronMessagingPeerGroup [peer-config]
  component/Lifecycle
  (start [component]
    (println "Start aeron")
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [bind-addr (common/bind-addr peer-config)
          external-addr (common/external-addr peer-config)
          port (:onyx.messaging/peer-port peer-config)
          ticket-counters (atom {})
          embedded-media-driver (component/start (->EmbeddedMediaDriver peer-config))]
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :ticket-counters ticket-counters
             :embedded-media-driver embedded-media-driver
             :port port)))

  (stop [{:keys [embedded-media-driver] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop embedded-media-driver)
    (assoc component :embedded-media-driver nil :bind-addr nil 
           :external-addr nil :external-channel nil :ticket-counters nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessagingPeerGroup {:peer-config peer-config}))

(defn subscription-ticket 
  [{:keys [replica-version ticket-counters] :as messenger} 
   {:keys [dst-task-id src-peer-id] :as sub}]
  (get-in @ticket-counters [replica-version [src-peer-id dst-task-id]]))

(defn subscription-aligned?
  [sub-ticket]
  (empty? (:aligned sub-ticket)))

(defn is-next-barrier? [messenger barrier]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (inc (m/epoch messenger)) (:epoch barrier))))

(defn found-next-barrier? [messenger {:keys [barrier] :as subscriber}]
  (let [barrier-val @barrier] 
    (and (is-next-barrier? messenger barrier-val) 
         (not (:emitted? barrier-val)))))

(defn unblocked? [messenger {:keys [barrier] :as subscriber}]
  (info "Unblocked?" subscriber (let [barrier-val @barrier] 
    (and (= (m/replica-version messenger) (:replica-version barrier-val))
         (= (m/epoch messenger) (:epoch barrier-val))
         (:emitted? barrier-val))))
  (let [barrier-val @barrier] 
    (and (= (m/replica-version messenger) (:replica-version barrier-val))
         (= (m/epoch messenger) (:epoch barrier-val))
         (:emitted? barrier-val))))

;; TODO, do not re-ify on every read
(defn controlled-fragment-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

;; TODO, should probably check against slot id for safety's sake in case we merge streams later
(defn handle-read-segments
  [messenger results barrier ticket dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)
        ;; FIXME, why 2?
        n-desired-messages 2
        ticket-val @ticket
        position (.position header)
        ret (cond (and (or (not= (:dst-task-id message) dst-task-id)
                           (not= (:src-peer-id message) src-peer-id))
                       (= (:replica-version message)
                          (m/replica-version messenger)))
                  ControlledFragmentHandler$Action/COMMIT

                  (< (:replica-version message)
                     (m/replica-version messenger))
                  ControlledFragmentHandler$Action/COMMIT

                  (> (:replica-version message)
                     (m/replica-version messenger))
                  ControlledFragmentHandler$Action/ABORT

                  (>= (count results) n-desired-messages)
                  ControlledFragmentHandler$Action/ABORT

                  (and (message? message)
                       (not (nil? @barrier))
                       (< ticket-val position))
                  (do 
                   (assert (= (m/replica-version messenger) (:replica-version message)))
                   ;; FIXME, not sure if this logically works.
                   ;; If ticket gets updated in mean time, then is this always invalid and should be continued?
                   ;; WORK OUT ON PAPER
                   (when (compare-and-set! ticket ticket-val position)
                     (do 
                      (assert (coll? (:payload message)))
                      (reduce conj! results (:payload message))))
                   ControlledFragmentHandler$Action/COMMIT)

                  (and (barrier? message)
                       (is-next-barrier? messenger message))
                  (do
                   ;(println "Got barrier " message)
                   (if (zero? (count results)) ;; empty? broken on transients
                     (do 
                      (reset! barrier message)
                      ControlledFragmentHandler$Action/BREAK)  
                     ControlledFragmentHandler$Action/ABORT))

                  :else
                  (throw (Exception. "Should not happen?")))]
    (println [:handle-message dst-task-id src-peer-id] (.position header) message ret)
    (add-tracked-message! messenger dst-task-id src-peer-id message ret [:handle-message dst-task-id src-peer-id])
    ret))

(defn poll-messages! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription barrier]} sub-info
        sub-ticket (subscription-ticket messenger sub-info)]
    ;; May not need to check for alignment here, can prob just do in :recover
    (if (subscription-aligned? sub-ticket)
      (if (unblocked? messenger sub-info)
        ;; Poll for new messages and barriers
        (let [results (transient [])
              ;; FIXME, maybe shouldn't reify a controlled fragment handler each time?
              ;; Put the fragment handler in the sub info?
              fh (controlled-fragment-data-handler
                  (fn [buffer offset length header]
                    (handle-read-segments messenger results barrier (:ticket sub-ticket) dst-task-id src-peer-id buffer offset length header)))]
          (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
          (persistent! results)))
      [])))

(defn handle-poll-new-barrier
  [messenger barrier dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)
        ret (cond (< (:replica-version message)
                     (m/replica-version messenger))
                  ControlledFragmentHandler$Action/COMMIT

                  (and (or (not= (:dst-task-id message) dst-task-id)
                           (not= (:src-peer-id message) src-peer-id))
                       (= (:replica-version message) (m/replica-version messenger)))
                  ControlledFragmentHandler$Action/COMMIT

                  (and (barrier? message)
                       (is-next-barrier? messenger message))
                  (do 
                   (reset! barrier message)
                   ControlledFragmentHandler$Action/BREAK)

                  :else
                  ControlledFragmentHandler$Action/ABORT)]
    (println [:poll-barrier dst-task-id src-peer-id] (.position header) message ret)
    (add-tracked-message! messenger dst-task-id src-peer-id message ret [:poll-new-barrier dst-task-id src-peer-id])
    ret))

(defn poll-new-replica! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription barrier]} sub-info
        sub-ticket (subscription-ticket messenger sub-info)]
    ;; May not need to check for alignment here, can prob just do in :recover
    (println "Poll new replica, aligned" (subscription-aligned? sub-ticket))
    (if (subscription-aligned? sub-ticket)
      (let [fh (controlled-fragment-data-handler
                (fn [buffer offset length header]
                  (handle-poll-new-barrier messenger barrier dst-task-id src-peer-id buffer offset length header)))]
        (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver))
      (info "SUB NOT ALIGNED"))))

(defn handle-poll-acks [messenger barrier-ack dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)
        ret (cond (< (:replica-version message) 
                     (m/replica-version messenger))
                  ControlledFragmentHandler$Action/COMMIT

                  (and (or (not= (:dst-task-id message) dst-task-id)
                           (not= (:src-peer-id message) src-peer-id))
                       (= (:replica-version message) 
                          (m/replica-version messenger)))
                  ControlledFragmentHandler$Action/COMMIT

                  (> (:replica-version message) 
                     (m/replica-version messenger))
                  ControlledFragmentHandler$Action/ABORT

                  (ack? message)
                  (do 
                   (reset! barrier-ack message)
                   ControlledFragmentHandler$Action/BREAK)

                  :else
                  (throw (Exception. "Shouldn't be any non ack messages in the stream")))]
    (println [:poll-acks dst-task-id src-peer-id] (.position header) message ret)
    (add-tracked-message! messenger dst-task-id src-peer-id message ret [:poll-acks dst-task-id src-peer-id])
    ret))

;; TODO, can possibly take more than one ack at a time from a sub?
; (defn handle-poll-acks [messenger barrier-ack dst-task-id src-peer-id buffer offset length header]
;   (let [ba (byte-array length)
;         _ (.getBytes ^UnsafeBuffer buffer offset ba)
;         message (messaging-decompress ba)]
;     (if (and (= (:dst-task-id message) dst-task-id)
;              (= (:src-peer-id message) src-peer-id)
;              (ack? message)
;              (= (m/replica-version messenger) (:replica-version message)))
;       (do 
;        (info "GOT NEW BARRIER ACK" (into {} message))
;        (reset! barrier-ack message)
;        ControlledFragmentHandler$Action/BREAK)
;       ControlledFragmentHandler$Action/COMMIT)))

(defn poll-acks! [messenger sub-info]
  (if @(:barrier-ack sub-info)
    (do
     ;(println "Poll acks got already ")
     messenger)
    (let [{:keys [src-peer-id dst-task-id subscription barrier-ack ticket-counter]} sub-info
          ;_ (println "Poll acks going to")
          fh (controlled-fragment-data-handler
              (fn [buffer offset length header]
                (handle-poll-acks messenger barrier-ack dst-task-id src-peer-id buffer offset length header)))]
      (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
      messenger)))

(defn handle-drain
  [sub-info buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)]
    (println "DRAIN MESSAGE position" (.position header) "sub" sub-info "message" message)
    ControlledFragmentHandler$Action/COMMIT))

(defn poll-drain-debug [sub-info image]
  (.println (System/out) (str "SUB" sub-info "imaage" image))
  (let [fh (controlled-fragment-data-handler
            (fn [buffer offset length header]
              (handle-drain sub-info buffer offset length header)))]
    (while (not (zero? (.controlledPoll image ^ControlledFragmentHandler fh fragment-limit-receiver))))))

(defn unavailable-image-drainer [sub-info]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      (println "UNAVAILABLE image" (.sessionId image) (.position image) sub-info)
      (poll-drain-debug sub-info image))))

(defn available-image [sub-info]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (println "AVAILABLE image" (.sessionId image) (.position image) sub-info))))

(defn new-subscription 
  [{:keys [messenger-group id] :as messenger}
   {:keys [job-id src-peer-id dst-task-id slot-id site] :as sub-info}]
  (info "new subscriber for " job-id src-peer-id dst-task-id)
  (let [sub-hash (hash [src-peer-id dst-task-id slot-id site])
        _ (println "NEW SUB" sub-info "hash" sub-hash)
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          (println "Aeron messaging subscriber error" x)
                          ;; FIXME: Reboot peer
                          (taoensso.timbre/warn x "Aeron messaging subscriber error")))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler)
                (.availableImageHandler (available-image sub-info))
                (.unavailableImageHandler (unavailable-image-drainer sub-info)))
        conn (Aeron/connect ctx)
        bind-addr (:bind-addr messenger-group)
        port (:port messenger-group)
        channel (mc/aeron-channel bind-addr port)
        stream (stream-id job-id dst-task-id slot-id site)
        _ (println "Add subscription " channel stream conn)
        subscription (.addSubscription conn channel stream)]
    (assoc sub-info
           :subscription subscription
           :stream stream
           :conn conn
           :hash sub-hash
           :barrier-ack (atom nil)
           :barrier (atom nil))))

(defn new-publication [{:keys [messenger-group] :as messenger}
                       {:keys [job-id src-peer-id dst-task-id slot-id site] :as pub-info}]
  (let [channel (mc/aeron-channel (:address site) (:port site))
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          ;; FIXME: Reboot peer
                          (println "Aeron messaging publication error" x)
                          (taoensso.timbre/warn "Aeron messaging publication error:" x)))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        stream (stream-id job-id dst-task-id slot-id site)
        _ (println "Creating new pub" channel stream)
        pub (.addPublication conn channel stream)]
    (assoc pub-info :conn conn :publication pub :stream stream)))

(defn add-to-subscriptions [subscriptions sub-info]
  (conj (or subscriptions []) sub-info))

(defn close-sub! [sub-info]
  (println "CLOSED SUB" sub-info)
  (.close ^Subscription (:subscription sub-info))
  (.close (:conn sub-info)))


(defn equiv-sub [sub-info1 sub-info2]
  (= (select-keys sub-info1 [:src-peer-id :dst-task-id :slot-id]) 
     (select-keys sub-info2 [:src-peer-id :dst-task-id :slot-id])))

(defn remove-from-subscriptions 
  [subscriptions {:keys [dst-task-id slot-id] :as sub-info}]
  {:post [(= (dec (count subscriptions)) (count %))]}
  (let [to-remove (first (filter (partial equiv-sub sub-info) subscriptions))] 
    (assert to-remove)
    (close-sub! to-remove)
    (vec (remove #{to-remove} subscriptions))))

(defn close-pub! [pub-info]
  (println "CLOSED PUB" pub-info)
  (.close ^Publication (:publication pub-info))
  (.close (:conn pub-info)))

(defn equiv-pub [pub-info1 pub-info2]
  (= (select-keys pub-info1 [:src-peer-id :dst-task-id :slot-id :site]) 
     (select-keys pub-info2 [:src-peer-id :dst-task-id :slot-id :site])))

(defn remove-from-publications [publications pub-info]
  {:post [(= (dec (count publications)) (count %))]}
  (let [to-remove (first (filter (partial equiv-pub pub-info) publications))] 
    (assert to-remove)
    (close-pub! to-remove)
    (vec (remove #{to-remove} publications))))

;; TICKETS SHOULD USE session id (unique publication) and position
;; Lookup task, then session id, then position, skip over positions that are lower, use ticket to take higher
;; Stick tickets in peer messenger group in single atom?
;; Have tickets be cleared up when image is no longer available?
;; Use these to manage tickets
;; onAvailableImage
;; onUnavailableImage

(defn flatten-publications [publications]
  (reduce (fn [all [dst-task-id ps]]
            (into all (mapcat (fn [[slot-id pubs]]
                                pubs)
                              ps)))
          []
          publications))

(defn set-barrier-emitted! [subscriber]
  (assert (not (:emitted? (:barrier subscriber))))
  (swap! (:barrier subscriber) assoc :emitted? true))

(defn allocation-changed? [replica job-id replica-version]
  (and (some #{job-id} (:jobs replica))
       (not= replica-version
             (get-in replica [:allocation-version job-id]))))


(defrecord AeronMessenger [messenger-group ticket-counters id replica-version epoch 
                           publications subscriptions ack-subscriptions read-index]
  component/Lifecycle
  (start [component]
    (assoc component
           :ticket-counters 
           (:ticket-counters messenger-group)))

  (stop [component]
    (reduce m/remove-publication component (flatten-publications publications))
    (reduce m/remove-subscription component subscriptions)
    (reduce m/remove-ack-subscription component ack-subscriptions)
    (assoc component 
           :ticket-counters nil :replica-version nil 
           :epoch nil :publications nil :subscription nil 
           :ack-subscriptions nil :read-index nil :!replica nil))

  m/Messenger
  (publications [messenger]
    (flatten-publications publications))

  (subscriptions [messenger]
    subscriptions)

  (ack-subscriptions [messenger]
    ack-subscriptions)

  (add-subscription [messenger sub-info]
    (update messenger :subscriptions add-to-subscriptions (new-subscription messenger sub-info)))

  (remove-subscription [messenger sub-info]
    (update messenger :subscriptions remove-from-subscriptions sub-info))

  (register-ticket [messenger sub-info]
    (swap! ticket-counters 
           update 
           replica-version 
           (fn [tickets]
             (update (or tickets {}) 
                     [(:src-peer-id sub-info)
                      (:dst-task-id sub-info)]
                     (fn [sub-ticket]
                       (if sub-ticket
                         ;; Already know what peers should be aligned
                         (update sub-ticket :aligned disj id)
                         {:ticket (atom -1)
                          :aligned (disj (set (:aligned-peers sub-info)) id)})))))
    messenger)

  (add-ack-subscription [messenger sub-info]
    (update messenger :ack-subscriptions add-to-subscriptions (new-subscription messenger sub-info)))

  (remove-ack-subscription [messenger sub-info]
    (update messenger :ack-subscriptions remove-from-subscriptions sub-info))

  (add-publication [messenger pub-info]
    (update-in messenger
               [:publications (:dst-task-id pub-info) (:slot-id pub-info)]
               (fn [pbs] 
                 (assert (= id (:src-peer-id pub-info)) [id (:src-peer-id pub-info)] )
                 (conj (or pbs []) 
                       (new-publication messenger pub-info)))))

  (remove-publication [messenger pub-info]
    (update-in messenger 
               [:publications (:dst-task-id pub-info) (:slot-id pub-info)] 
               remove-from-publications 
               pub-info))

  (set-replica-version [messenger replica-version]
    (reset! (:read-index messenger) 0)
    (run! (fn [sub] (reset! (:barrier-ack sub) nil)) ack-subscriptions)
    (run! (fn [sub] (reset! (:barrier sub) nil)) subscriptions)
    (-> messenger 
        (assoc :replica-version replica-version)
        (m/set-epoch 0)))

  (replica-version [messenger]
    (get messenger :replica-version))

  (epoch [messenger]
    epoch)

  (set-epoch [messenger epoch]
    (assoc messenger :epoch epoch))

  (next-epoch [messenger]
    (update messenger :epoch inc))

  (poll-acks [messenger]
    (reduce poll-acks! messenger ack-subscriptions))

  (all-acks-seen? 
    [messenger]
    (if (empty? (remove (comp deref :barrier-ack) ack-subscriptions))
      (select-keys @(:barrier-ack (first ack-subscriptions)) 
                   [:replica-version :epoch])))

  (flush-acks [messenger]
    (run! (fn [sub] (reset! (:barrier-ack sub) nil)) ack-subscriptions)
    messenger)

  (poll [messenger]
    (let [subscriber (get subscriptions (mod @(:read-index messenger) (count subscriptions)))
          messages (poll-messages! messenger subscriber)] 
      (swap! (:read-index messenger) inc)
      (mapv t/input messages)))

  (offer-segments [messenger batch {:keys [dst-task-id slot-id] :as task-slot}]
    ;; Problem here is that if no slot will accept the message we will
    ;; end up having to recompress on the next offer
    ;; Possibly should try more than one iteration before returning
    ;; TODO: should re-use unsafe buffers in aeron messenger. 
    ;; Will require nippy to be able to write directly to unsafe buffers
    (let [payload ^bytes (messaging-compress (->Message id dst-task-id slot-id replica-version batch))
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      ;; shuffle publication order to ensure even coverage. FIXME: slow
      (loop [pubs (shuffle (get-in publications [dst-task-id slot-id]))]
        (if-let [pub-info (first pubs)]
          (let [ret (.offer ^Publication (:publication pub-info) buf 0 (.capacity buf))]
            (println "Offer ret" ret)
            ;(println "Failed offer message" (neg? ret))
            (if (neg? ret)
              (recur (rest pubs))
              task-slot))))))

  (poll-recover [messenger]
    (when-not (= 1 (count 
            (set (map (fn [sub]
                        (set (map (fn [i] [(.isClosed i) (.correlationId i)]) 
                                  (.images (:subscription sub)))))
                      subscriptions))))
      (println "SOME SUBSCRIPTIONS ARE NOT ALL IN THE SAME STATE"))
    (when (= 1 (count 
                (set (map (fn [sub]
                            (set (map (fn [i] [(.isClosed i) (.correlationId i)]) 
                                      (.images (:subscription sub)))))
                          subscriptions))))
      (loop [sbs subscriptions]
        (let [sub (first sbs)] 
          (when sub 
            (println "WHEN SUB" (m/epoch messenger)
                     (.channel (:subscription sub))
                     (.streamId (:subscription sub))
                     (mapv (fn [i] [(.position i) (.correlationId i) (.sourceIdentity i)]) (.images (:subscription sub))) sub)
            (when-not @(:barrier sub)
              (poll-new-replica! messenger sub)
              (println "AFTER SUB" (m/epoch messenger)
                       (.channel (:subscription sub))
                       (.streamId (:subscription sub))

                       (mapv (fn [i] [(.position i) (.correlationId i) (.sourceIdentity i)]) (.images (:subscription sub))) sub)

              )
            (recur (rest sbs)))))
      (if (m/all-barriers-seen? messenger)
        (let [_ (info "ALL SEEN SUBS " (vec subscriptions))
              recover (:recover @(:barrier (first subscriptions)))] 
          (assert (= 1 (count (set (map (comp :recover deref :barrier) subscriptions)))))
          (assert recover)
          recover))))

  ;; FIXME RENAME TO OFFERBARRIER
  (emit-barrier [messenger publication]
    (onyx.messaging.messenger/emit-barrier messenger publication {}))

  ;; FIXME RENAME TO OFFERBARRIER
  (emit-barrier [messenger publication barrier-opts]
    (let [barrier (merge (->Barrier id (:dst-task-id publication) (m/replica-version messenger) (m/epoch messenger))
                         (assoc barrier-opts 
                                :site (:site publication)
                                :stream (:stream publication)
                                :new-id (java.util.UUID/randomUUID)))
          publication ^Publication (:publication publication)
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress barrier))]
      (let [ret (.offer ^Publication publication buf 0 (.capacity buf))] 
        (swap! sent-barriers conj barrier)
        (println "Sending barrier" barrier "from " id)
        ;(println "Failed offer barrier" (neg? ret))
        (println "barrier ret" ret)
        (if (neg? ret)
          :fail
          :success))))

  (unblock-subscriptions! [messenger]
    (run! set-barrier-emitted! subscriptions)
    messenger)

  (all-barriers-seen? [messenger]
    ; (println "All barriers seen" 
    ;          (m/replica-version messenger)
    ;          (m/epoch messenger)
    ;          (map (partial found-next-barrier? messenger) subscriptions)
    ;          subscriptions
    ;          (empty? (remove #(found-next-barrier? messenger %) 
    ;                          subscriptions)))
    (empty? (remove #(found-next-barrier? messenger %) 
                    subscriptions)))

  (emit-barrier-ack [messenger publication]
    (let [ack (->BarrierAck id 
                            (:dst-task-id publication) 
                            (m/replica-version messenger) 
                            (m/epoch messenger))
          publication ^Publication (:publication publication)
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress ack))]
      (let [ret (.offer ^Publication publication buf 0 (.capacity buf))] 
        (println "Sending ack to " publication ack)
        (println "Failed offer barrier ack" (neg? ret))
        (println "ack ret" ret)
        (if (neg? ret)
          :fail
          :success)))))

(defmethod m/build-messenger :aeron [peer-config messenger-group id]
  (map->AeronMessenger {:id id 
                        :peer-config peer-config 
                        :messenger-group messenger-group 
                        :read-index (atom 0)}))

(defmethod clojure.core/print-method AeronMessagingPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
