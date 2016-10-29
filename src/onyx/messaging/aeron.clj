(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Message ->Barrier]]
            [onyx.messaging.messenger :as m]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [io.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler FragmentAssembler]
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

(defn hash-sub [sub-info]
  (hash (select-keys sub-info [:src-peer-id :dst-task-id :slot-id :site])))

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

(defn stream-id [job-id task-id slot-id site src-peer-id]
  (hash [job-id task-id slot-id site #_src-peer-id]))

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod m/get-peer-site :aeron
  [peer-config]
  {:address (mc/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

(defn delete-aeron-directory-safe [^MediaDriver$Context media-driver-context]
  (try (.deleteAeronDirectory media-driver-context)
       (catch java.nio.file.NoSuchFileException nsfe
         (info "Couldn't delete aeron media dir. May have been already deleted by shutdown hook." nsfe))))

(def aeron-dir-name (atom nil))
;(reset! aeron-dir-name (str "/var/folders/d8/6x6y27ln2f702g56jzzh9h780000gn/T/aeron-lucas" (java.util.UUID/randomUUID)))

(defrecord EmbeddedMediaDriver [peer-config]
  component/Lifecycle
  (start [component]
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? peer-config)
          threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading peer-config))
          media-driver-context (if embedded-driver?
                                 (do
                                  ;(reset! aeron-dir-name (str "/var/folders/d8/6x6y27ln2f702g56jzzh9h780000gn/T/aeron-lucas-" (java.util.UUID/randomUUID)))
                                  (-> (MediaDriver$Context.) 
                                      (.threadingMode threading-mode)
                                      ;(.aeronDirectoryName @aeron-dir-name)
                                      (.dirsDeleteOnStart true))))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))]
      (when embedded-driver? 
        (.addShutdownHook (Runtime/getRuntime) 
                          (Thread. (partial delete-aeron-directory-safe media-driver-context))))
      (when embedded-driver? (println "MEDIADRIVER:" @aeron-dir-name))
      (assoc component 
             :media-driver media-driver 
             :media-driver-context media-driver-context)))
  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (when media-driver 
      (.close ^MediaDriver media-driver))
    #_(when media-driver-context 
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

(defn subscription-aligned?
  [sub-ticket]
  true
  #_(empty? (:aligned sub-ticket)))

(defn unblocked? [messenger {:keys [barrier] :as subscriber}]
  (let [barrier-val @barrier] 
    (and (= (m/replica-version messenger) (:replica-version barrier-val))
         (= (m/epoch messenger) (:epoch barrier-val))
         (:emitted? barrier-val))))

(defprotocol PartialSubscriber 
  (init [this])
  (set-ticket [this new-ticket])
  (set-replica-version! [this new-replica-version])
  (set-epoch! [this new-epoch])
  (block! [this])
  (unblock! [this])
  (blocked? [this])
  ;; REMOVE
  (get-recover [this])
  ;; REMOVE
  (reset-recover! [this])
  (get-batch [this]))

(defn action->kw [action]
  (cond (= action ControlledFragmentHandler$Action/CONTINUE)
        :CONTINUE
        (= action ControlledFragmentHandler$Action/BREAK)
        :BREAK
        (= action ControlledFragmentHandler$Action/ABORT)
        :ABORT
        (= action ControlledFragmentHandler$Action/COMMIT)
        :COMMIT))
  
(deftype RecoverFragmentHandler [src-peer-id dst-task-id 
                                 ;; REMOVE
                                 ^:unsynchronized-mutable recover
                                 ^:unsynchronized-mutable blocked
                                 ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch]
  PartialSubscriber
  (set-replica-version! [this new-replica-version]
    (set! replica-version new-replica-version)
    this)
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (init [this]
    this)
  (get-recover [this] recover)
  (reset-recover! [this] (set! recover nil))
  (blocked? [this] blocked)
  (block! [this] (set! blocked true))
  (unblock! [this] (set! blocked false))
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          rv-msg (:replica-version message)
          ret (cond (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (and (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:src-peer-id message) src-peer-id))
                         (= rv-msg replica-version))
                    ControlledFragmentHandler$Action/CONTINUE

                    (and 
                     ;; Remove
                     (barrier? message)
                     
                     (= rv-msg replica-version)
                         ;; allow greater later
                         (= (:epoch message) (inc epoch)))
                    (do 
                     (block! this)
                     (set! recover (:recover message))
                     (cond (barrier? message) 
                           ControlledFragmentHandler$Action/BREAK
                           (message? message)
                           ControlledFragmentHandler$Action/ABORT
                           :else
                           (throw (Exception. "ut oh"))))

                    :else
                    ControlledFragmentHandler$Action/ABORT)]
      (println [:recover (action->kw ret) :dst dst-task-id :src src-peer-id :rv replica-version :e epoch] (.position header) message)
      ;(add-tracked-message! messenger dst-task-id src-peer-id message ret [:poll-new-barrier dst-task-id src-peer-id])
      ret)))

;; TODO, can skip everything if it's the wrong image completely
(deftype ReadSegmentsFragmentHandler 
  [src-peer-id dst-task-id 
   ^:unsynchronized-mutable blocked ^:unsynchronized-mutable ticket ^:unsynchronized-mutable batch 
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch]
  PartialSubscriber
  (set-replica-version! [this new-replica-version]
    (set! replica-version new-replica-version)
    this)
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (init [this]
    (set! batch (transient []))
    this)
  (set-ticket [this new-ticket]
    (set! ticket new-ticket)
    this)
  (get-batch [this]
    (persistent! batch))
  (blocked? [this] blocked)
  (block! [this] 
    (set! blocked true))
  (unblock! [this] 
    (set! blocked false))
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    ;(println "ON FRAGMENT " ticket)
    (assert (not blocked))
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          ;; FIXME, why 2?
          n-desired-messages 2
          ticket-val @ticket
          position (.position header)
          rv-msg (:replica-version message)
          ret (cond (and (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:src-peer-id message) src-peer-id))
                         (= rv-msg replica-version))
                    ControlledFragmentHandler$Action/CONTINUE

                    (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (> rv-msg replica-version)
                    ControlledFragmentHandler$Action/ABORT

                    (>= (count batch) n-desired-messages)
                    ControlledFragmentHandler$Action/ABORT

                    (and (message? message)
                         (= replica-version (:replica-version message))
                         (= epoch (:epoch message))
                         (< ticket-val position))
                    (do 
                     (assert (= replica-version rv-msg))
                     ;; FIXME, not sure if this logically works.
                     ;; If ticket gets updated in mean time, then is this always invalid and should be continued?
                     ;; WORK OUT ON PAPER
                     (when true ;(compare-and-set! ticket ticket-val position)
                       (do 
                        (reduce conj! batch (:payload message))
                        (assert (coll? (:payload message)))))
                     ControlledFragmentHandler$Action/CONTINUE)

                    ;; Should only be used with messages when sharing a slot id
                    ;; Add assertion check here
                    (and 
                     ;; Should allow non barriers to allow messages to fill me in
                     (barrier? message)

                     (= replica-version (:replica-version message))
                     ;; allow to be greater later
                     (= (:epoch message) (inc epoch)))
                    (if (zero? (count batch)) ;; empty? broken on transients
                      (do 
                       (println "Blocking again thanks to " message "with" replica-version epoch)
                       (block! this) 
                       (if (barrier? message) 
                         ControlledFragmentHandler$Action/BREAK
                         ;; Allows us to skip over a barrier and still block correctly
                         ControlledFragmentHandler$Action/ABORT))
                      ControlledFragmentHandler$Action/ABORT)

                    (message? message)
                    ControlledFragmentHandler$Action/ABORT

                    :else
                    (throw (Exception. (str "Should not happen? " ticket-val " " position " " replica-version " " epoch " vs " (into {} message)))))]
      (println [:handle-message (action->kw ret) dst-task-id src-peer-id] (.position header) message)
      ;(add-tracked-message! messenger dst-task-id src-peer-id message ret [:handle-message dst-task-id src-peer-id])
      ret)))

(defn pub-info-meta [messenger {:keys [publication] :as pub-info}]
  [:rv (m/replica-version messenger)
   :e (m/epoch messenger)
   :session-id (.sessionId publication) 
   :stream-id (.streamId publication)
   :pos (.position publication)
   :pub-info (select-keys pub-info [:src-peer-id :dst-task-id :slot-id :site])])

(defn sub-info-meta [messenger {:keys [subscription barrier] :as sub-info}]
  [:rv (m/replica-version messenger)
   :e (m/epoch messenger)
   :unblocked? (boolean (unblocked? messenger sub-info))
   :barrier-rv-e (if-let [b @barrier]
                   [(:replica-version b) (:epoch b)])
   :sub-hash (hash-sub sub-info)
   :channel-id (.channel subscription)
   :registation-id (.registrationId subscription)
   :stream-id (.streamId subscription)
   :closed? (.isClosed subscription)
   :images (mapv (fn [i] [:pos (.position i) 
                          :term-id (.initialTermId i) 
                          :session-id (.sessionId i) 
                          :closed? (.isClosed i) 
                          :corr-id (.correlationId i) 
                          :source-id (.sourceIdentity i)]) 
                 (.images subscription)) 
   :sub-info (select-keys sub-info [:src-peer-id :dst-task-id :slot-id :site])])

(defn poll-messages! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription aligned-peers]} sub-info]
    (println "poll-messages!:" (sub-info-meta messenger sub-info))
    ;; May not need to check for alignment here, can prob just do in :recover
    ;; Make this a function of the segments-handler?
    (let [handler (-> sub-info
                      :segments-handler
                      (init))
          assembler (:segments-assembler sub-info)
          images (.images ^Subscription subscription)]
      ;; Possibly should poll other images that we're not interested in still
      (if-not (blocked? handler) 
        (do (run! (fn [img]
                    (let [ticket (get-in @(m/ticket-counters messenger) [(.correlationId img) :ticket])] 
                      (.controlledPoll ^Image img 
                                       ^ControlledFragmentHandler (set-ticket assembler ticket)
                                       fragment-limit-receiver)))
                  images)
            (let [batch (get-batch handler)]
              (println "polled batch:" batch)
              batch))
        []))))

(defn poll-new-replica! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription barrier aligned-peers]} sub-info
        _ (println "Poll replica")
        _ (println "poll-new-replica!, before:" (.id messenger) (sub-info-meta messenger sub-info))
        recover-handler (-> sub-info
                            :recover-handler
                            init)
        assembler (:recover-assembler sub-info)
        images (.images ^Subscription subscription)]
    (when-not (blocked? recover-handler) 
      (run! (fn [img]
              (.controlledPoll ^Image img ^ControlledFragmentHandler assembler fragment-limit-receiver))
            images)
      (println "Checking" sub-info "blocked?" (blocked? (:segments-handler sub-info)))
      ;; blocking segment handler?
      ;(block! (:recover-handler sub-info))
      (when (blocked? recover-handler)
        (assert (blocked? (:segments-handler sub-info)))
        (println "Unblocking" sub-info)
        (unblock! (:segments-handler sub-info))))
    (println "poll-new-replica!, after" (.id messenger) (sub-info-meta messenger sub-info))))

(defn handle-drain
  [sub-info buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)]
    ;(println "DRAIN MESSAGE SUB hash" (hash-sub sub-info) sub-info "message" message)
    ControlledFragmentHandler$Action/CONTINUE))

(defn unavailable-image [sub-info ticket-counters]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      ;; Cleanup unused tickets
      (.println (System/out) (str "UNAVAILABLE image:" :corr (.correlationId image) :pos (.position image) " " (.sessionId image) " " sub-info)))))

(defn available-image [id sub-info ticket-counters]
  (reify AvailableImageHandler
    (onAvailableImage [this image]
      (swap! ticket-counters 
             update 
             (.correlationId image)
             (fn [sub-ticket]
               (if sub-ticket
                 (update sub-ticket :aligned conj id)
                 {:ticket (atom -1)
                  :aligned #{id}})))
      (.println (System/out) (str "AVAILABLE image:" :corr (.correlationId image) :pos (.position image) " " (.sessionId image) " " sub-info)))))

(defn new-subscription 
  [messenger {:keys [job-id src-peer-id dst-task-id slot-id site] :as sub-info}]
  (let [messenger-group (.messenger-group messenger)
        ticket-counters (m/ticket-counters messenger)
        id (.id messenger)
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          (println "Aeron messaging subscriber error" x)
                          ;(System/exit 1)
                          ;; FIXME: Reboot peer
                          (taoensso.timbre/warn x "Aeron messaging subscriber error")))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler)
                ;(.aeronDirectoryName @aeron-dir-name)
                (.availableImageHandler (available-image id sub-info ticket-counters))
                (.unavailableImageHandler (unavailable-image sub-info ticket-counters)))
        conn (Aeron/connect ctx)
        _ (assert messenger-group)
        bind-addr (:bind-addr messenger-group)
        port (:port messenger-group)
        channel (mc/aeron-channel bind-addr port)
        stream (stream-id job-id dst-task-id slot-id site src-peer-id)
        subscription (.addSubscription conn channel stream)
        barrier (atom nil)
        recover-fragment-handler (RecoverFragmentHandler. src-peer-id dst-task-id nil nil nil nil)
        recover-assembler (ControlledFragmentAssembler. recover-fragment-handler)
        segments-fragment-handler (ReadSegmentsFragmentHandler. src-peer-id dst-task-id nil nil nil nil nil)
        segments-assembler (ControlledFragmentAssembler. segments-fragment-handler)
        sub-info (assoc sub-info
                        :subscription subscription 
                        :stream stream 
                        :conn conn
                        :recover-handler recover-fragment-handler
                        :recover-assembler recover-assembler
                        :segments-handler segments-fragment-handler
                        :segments-assembler segments-fragment-handler
                        :barrier barrier)]
    (println "New sub:" (sub-info-meta messenger sub-info))
    sub-info))

(defn new-publication [messenger messenger-group {:keys [job-id src-peer-id dst-task-id slot-id site] :as pub-info}]
  (assert src-peer-id)
  (let [channel (mc/aeron-channel (:address site) (:port site))
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          ;(System/exit 1)
                          ;; FIXME: Reboot peer
                          (println "Aeron messaging publication error" x)
                          (taoensso.timbre/warn "Aeron messaging publication error:" x)))
        ctx (-> (Aeron$Context.)
                ;(.aeronDirectoryName @aeron-dir-name)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        stream (stream-id job-id dst-task-id slot-id site src-peer-id)
        pub (.addPublication conn channel stream)
        pub-info (assoc pub-info :conn conn :publication pub :stream stream)]
    (println "New pub:" (pub-info-meta messenger pub-info))
    pub-info))

(defn add-to-subscriptions [subscriptions sub-info]
  (conj (or subscriptions []) sub-info))

(defn close-sub! [sub-info]
  (println "Close sub:" sub-info)
  (.close ^Subscription (:subscription sub-info))
  (.close (:conn sub-info)))

(defn sub-id [sub-info]
  (select-keys sub-info [:src-peer-id :dst-task-id :slot-id :site]))

(defn equiv-sub [sub-info1 sub-info2]
  (= (sub-id sub-info1) 
     (sub-id sub-info2)))

(defn remove-from-subscriptions 
  [subscriptions {:keys [dst-task-id slot-id] :as sub-info}]
  {:post [(= (dec (count subscriptions)) (count %))]}
  (let [to-remove (first (filter (partial equiv-sub sub-info) subscriptions))] 
    (assert to-remove)
    (close-sub! to-remove)
    (vec (remove #{to-remove} subscriptions))))

(defn close-pub! [pub-info]
  (println "Close pub:" pub-info)
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

(defn reconcile-sub [messenger old-sub new-sub]
  (cond (and old-sub (nil? new-sub))
        (do (close-sub! old-sub)
            nil)
        (and (nil? old-sub) new-sub)
        (new-subscription messenger new-sub)
        :else
        (merge old-sub new-sub)))

(defn transition-subscriptions [messenger prev nxt]
  (let [m-prev (into {} 
                     (map (juxt sub-id identity))
                     prev)
        m-next (into {} 
                     (map (juxt sub-id identity))
                     nxt)
        all-keys (into (set (keys m-prev)) 
                       (keys m-next))]
    (vec (keep (fn [k]
                 (let [old (m-prev k)
                       new (m-next k)]
                   (reconcile-sub messenger old new)))
               all-keys))))

(deftype AeronMessenger [messenger-group 
                         id 
                         ^:unsynchronized-mutable ticket-counters 
                         ^:unsynchronized-mutable replica-version 
                         ^:unsynchronized-mutable epoch 
                         ^:unsynchronized-mutable publications 
                         ^:unsynchronized-mutable subscriptions 
                         ^:unsynchronized-mutable read-index]
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    (reduce m/remove-publication component (flatten-publications publications))
    (run! close-sub! subscriptions)
    (set! ticket-counters nil)
    (set! replica-version -1)
    (set! epoch -1)
    (set! publications nil)
    (set! subscriptions nil)
    component)

  m/Messenger
  (publications [messenger]
    (flatten-publications publications))

  (subscriptions [messenger]
    subscriptions)

  (update-subscriptions [messenger sub-infos]
    (set! subscriptions (transition-subscriptions messenger subscriptions sub-infos))
    messenger)

  ; (add-subscription [messenger sub-info]
  ;   ;; TODO: add-subscription should just update-subscription. If it doesn't exist then we add it. If it does exist, we update it
  ;   ;; We can also update the replica version here, and set-replica-version first
  ;   (set! subscriptions (add-to-subscriptions subscriptions (new-subscription messenger messenger-group ticket-counters sub-info)))
  ;   messenger)

  ; (remove-subscription [messenger sub-info]
  ;   (let [new-subs (remove-from-subscriptions subscriptions sub-info)]
  ;     (println "New subs" new-subs)
  ;     (set! subscriptions new-subs))
  ;   messenger)

  (ticket-counters [messenger] 
    ticket-counters)

  (register-ticket [messenger sub-info]
    ;(assert (<= (count (:aligned-peers sub-info)) 1))
    ;; TODO, clear previous versions at some point? Have to worry about other threads though
    #_(swap! ticket-counters 
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
    (println "Registered ticket " @ticket-counters)
    messenger)

  (get-ticket [messenger {:keys [dst-task-id src-peer-id] :as sub}]
    (get-in @ticket-counters [replica-version [src-peer-id dst-task-id]]))

  (add-publication [messenger pub-info]
    (set! publications 
          (update-in publications
                     [(:dst-task-id pub-info) (:slot-id pub-info)]
                     (fn [pbs] 
                       (assert (= id (:src-peer-id pub-info)) [id (:src-peer-id pub-info)] )
                       (conj (or pbs []) 
                             (new-publication messenger messenger-group pub-info)))))

    messenger)

  (remove-publication [messenger pub-info]
    (set! publications (update-in publications 
                                  [(:dst-task-id pub-info) (:slot-id pub-info)] 
                                  remove-from-publications 
                                  pub-info))
    messenger)

  (set-replica-version! [messenger rv]
    (set! read-index 0)
    ; unblock subscriptions
    (println "Subscriptions" subscriptions)
    ;(run! (fn [sub] (reset! (:barrier sub) nil)) subscriptions)
    (set! replica-version rv)
    (m/set-epoch! messenger 0)
    (run! (fn [sub]
            (reset-recover! (:recover-handler sub))
            (unblock! (:recover-handler sub))
            (set-replica-version! (:recover-handler sub) rv)
            (block! (:segments-handler sub))
            (set-replica-version! (:segments-handler sub) rv)) 
          subscriptions)
    (reduce m/register-ticket messenger subscriptions))

  (replica-version [messenger]
    replica-version)

  (epoch [messenger]
    epoch)

  (set-epoch! [messenger e]
    (println "Set epoch" id replica-version e epoch)
    (run! (fn [sub]
            (println "Unblockign segment handler")
            ;; FIXME, interacts with set-replica-version badly. Maybe don't unblock here
            (unblock! (:segments-handler sub))
            (set-epoch! (:segments-handler sub) e)
            (set-epoch! (:recover-handler sub) e)) 
          subscriptions)
    (set! epoch e)
    messenger)

  (next-epoch! [messenger]
    (println "Next epoch" id)
    (m/set-epoch! messenger (inc epoch)))

  (poll [messenger]
    ;; TODO, poll all subscribers in one poll?
    ;; TODO, test for overflow?
    (let [subscriber (get subscriptions (mod read-index (count subscriptions)))
          messages (poll-messages! messenger subscriber)] 
      (set! read-index (inc read-index))
      (mapv t/input messages)))

  (offer-segments [messenger batch {:keys [dst-task-id slot-id] :as task-slot}]
    ;; Problem here is that if no slot will accept the message we will
    ;; end up having to recompress on the next offer
    ;; Possibly should try more than one iteration before returning
    ;; TODO: should re-use unsafe buffers in aeron messenger. 
    ;; Will require nippy to be able to write directly to unsafe buffers
    (let [message (->Message id dst-task-id slot-id replica-version epoch batch)
          payload ^bytes (messaging-compress message)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      ;; shuffle publication order to ensure even coverage. FIXME: slow
      ;; FIXME, don't use SHUFFLE AS IT FCKS WITH REPRO. Also slow
      (loop [pubs (shuffle (get-in publications [dst-task-id slot-id]))]
        (if-let [pub-info (first pubs)]
          (let [ret (.offer ^Publication (:publication pub-info) buf 0 (.capacity buf))]
            (println "Offer segment" [:ret ret :message message :pub (pub-info-meta messenger pub-info)])
            (if (neg? ret)
              (recur (rest pubs))
              task-slot))))))

  (poll-recover [messenger]
    (loop [sbs subscriptions]
      (let [sub (first sbs)] 
        (when sub 
          (poll-new-replica! messenger sub) 
          (recur (rest sbs)))))
    (debug "Seen all subs?: " (m/all-barriers-seen? messenger) :subscriptions (mapv sub-info-meta subscriptions))
    (if (m/all-barriers-seen? messenger)
      (let [recover (get-recover (:recover-handler (first subscriptions)))] 
        (println "Recover" recover)
        (assert recover)
        recover)))

  (offer-barrier [messenger pub-info]
    (onyx.messaging.messenger/offer-barrier messenger pub-info {}))

  (offer-barrier [messenger pub-info barrier-opts]
    (let [barrier (merge (->Barrier id (:dst-task-id pub-info) (m/replica-version messenger) (m/epoch messenger))
                         (assoc barrier-opts 
                                :site (:site pub-info)
                                :stream (:stream pub-info)
                                :new-id (java.util.UUID/randomUUID)))
          publication ^Publication (:publication pub-info)
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress barrier))]
      (let [ret (.offer ^Publication publication buf 0 (.capacity buf))] 
        (println "Offer barrier:" [:ret ret :message barrier :pub (pub-info-meta messenger pub-info)])
        ret)))

  ;; FIXME REMOVE OR RATIONALISE
  (unblock-subscriptions! [messenger]
    ;(run! (comp block! :segments-handler) subscriptions)
    messenger)

  ;; rename - all subscriptions-blocked?
  (all-barriers-seen? [messenger]
    ;; Simplify after recover is done
    (mapv (fn [s]
            (assert (or (not (blocked? (:recover-handler s)))
                        (get-recover (:recover-handler s)))))
            subscriptions)
    (println "Blockage"
             (empty? (remove #(blocked? (:recover-handler %)) 
                             subscriptions))
             (empty? (filter #(blocked? (:segments-handler %)) 
                     subscriptions))   
             )
    (and (empty? (remove #(blocked? (:recover-handler %)) 
                         subscriptions))
         (empty? (filter #(blocked? (:segments-handler %)) 
                         subscriptions))))

  (all-barriers-completed? [messenger]
    (println "all bompleted?"
             false)
    false))

(defmethod m/build-messenger :aeron [peer-config messenger-group id]
  (->AeronMessenger messenger-group id (:ticket-counters messenger-group) -1 -1 nil nil 0))

(defmethod clojure.core/print-method AeronMessagingPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
