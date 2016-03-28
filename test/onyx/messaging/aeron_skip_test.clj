(ns onyx.messaging.aeron-skip-test
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info] :as timbre]
            [clojure.test :refer [deftest is testing]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context ControlledFragmentAssembler FragmentAssembler Publication Subscription]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer FragmentHandler ControlledFragmentHandler ControlledFragmentHandler$Action]
           [uk.co.real_logic.agrona ErrorHandler]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [onyx_codec MessageContainerEncoder MessageContainerDecoder]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(def no-op-error-handler
  (reify ErrorHandler
    (onError [this x] (spit "myerror.txt" (pr-str x)))))

; (defn handle-message [result-state this-task-id buffer offset length header]
;   (let [ba (byte-array length)
;         _ (.getBytes buffer offset ba)
;         res (messaging-decompress ba)]
;     (cond (and (record? res) (= this-task-id (:dst-task res)))
;           (do (swap! result-state conj (map->Barrier (into {} res)))
;               ControlledFragmentHandler$Action/BREAK)

;           (and (record? res) (not= this-task-id (:dst-task res)))
;           ControlledFragmentHandler$Action/CONTINUE

;           (coll? res)
;           (do (doseq [m res]
;                 (when (= (:dst-task m) this-task-id)
;                   (swap! result-state conj m)))
;               ControlledFragmentHandler$Action/CONTINUE)

;           (:barrier-id res)
;           (comment "pass")

;           :else (throw (ex-info "Not sure what happened" {})))))

; (defn handle-message [buffer offset length header]
;   ControlledFragmentHandler$Action/CONTINUE)

(defn controlled-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn data-handler [f]
  (FragmentAssembler.
    (reify FragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

; (defmethod extensions/receive-messages AeronMessenger
;   [subscription]
;   (let [fh (fragment-data-handler (partial handle-message result-state (:onyx.core/task-id event)))
;         n-fragments (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh 10)]
;     @result-state))

; (defn start-stream-observer! [conn bind-addr port stream-id idle-strategy event task-id]
;   (let [channel (aeron-channel bind-addr port)
;         subscription (.addSubscription conn channel stream-id)
;         handler (fragment-data-handler
;                  (fn [buffer offset length header]
;                    (stream-observer-handler event task-id buffer offset length header)))
;         subscription-fut (future (try (.accept ^Consumer (consumer handler idle-strategy 10) subscription)
;                                     (catch Throwable e (fatal e))))]
;     {:subscription subscription
;      :subscription-fut subscription-fut}))


(deftest test-speed
  (let [media-driver-context (-> (MediaDriver$Context.) 
                                 ;(.threadingMode ThreadingMode/SHARED)
                                 (.dirsDeleteOnStart true))
        media-driver ^MediaDriver (MediaDriver/launch media-driver-context)]
    (try
     (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
           conn (Aeron/connect ctx)
           channel (aeron-channel "127.0.0.1" 40200)
           stream-id 1
           publication1 (.addPublication conn channel stream-id)
           publication2 (.addPublication (Aeron/connect ctx) channel stream-id)
           _ (println "Termbuffer length " (.termBufferLength publication1))
           subscription1 (.addSubscription conn channel stream-id)
           ;subscription2 (.addSubscription conn channel stream-id)
           ;subscription3 (.addSubscription conn channel stream-id)
           ;subscription4 (.addSubscription conn channel stream-id)
           ;subscription5 (.addSubscription conn channel stream-id)
           ;subscription6 (.addSubscription conn channel stream-id)
           ;subscription7 (.addSubscription conn channel stream-id)
           ;subscription8 (.addSubscription conn channel stream-id)
           size 40
           bs (byte-array size)
           buf (UnsafeBuffer. bs)
           sent-messages (atom 0)
           read-messages (atom 0)
           handle-message (fn [buffer offset length header]
                            (swap! read-messages inc))
           controlled-handle-message (fn [buffer offset length header]
                                       (swap! read-messages inc)
                                       ControlledFragmentHandler$Action/CONTINUE)
           controlled-handler (controlled-data-handler controlled-handle-message)
           fragment-assembler (data-handler handle-message)
           _ (while (not (= -2 (.offer ^Publication publication1 buf 0 size))) 
               (swap! sent-messages inc))
           _ (println "Sent 1 " @sent-messages)
           _ (println "Limits " 
                      (.positionLimit publication1) 
                      (.positionLimit publication2)
                      (.position publication1)
                      (.position publication2))
           _ (while (not (= -2 (.offer ^Publication publication2 buf 0 size))) 
               (swap! sent-messages inc))
           _ (println "Sent 2 " @sent-messages)
           offer-fut 
           (future
                      #_(while (not (Thread/interrupted)) 
                        (let [offer-result (.offer ^Publication publication1 buf 0 size)]
                          (when-not (neg? offer-result)
                            (swap! sent-messages inc)))))

           ;;; You're allowed to produce to a publication if the previous barrier was consumed or you're below X%

           receive-fut (future
                        (while (not (Thread/interrupted)) 
                          (.controlledPoll ^Subscription subscription1 ^ControlledFragmentHandler controlled-handler 10)
                          (println "Limits " 
                                   (.positionLimit publication1) 
                                   (.positionLimit publication2)
                                   (.position publication1)
                                   (.position publication2))
                          ;(.controlledPoll ^Subscription subscription2 ^ControlledFragmentHandler controlled-handler 10)
                          ;(.controlledPoll ^Subscription subscription3 ^ControlledFragmentHandler controlled-handler 10)
                          ;(.controlledPoll ^Subscription subscription4 ^ControlledFragmentHandler controlled-handler 10)
                          ;(.controlledPoll ^Subscription subscription5 ^ControlledFragmentHandler controlled-handler 10)
                          ;(.controlledPoll ^Subscription subscription6 ^ControlledFragmentHandler controlled-handler 10)
                          ;(.controlledPoll ^Subscription subscription7 ^ControlledFragmentHandler controlled-handler 10)
                          ;(.controlledPoll ^Subscription subscription8 ^ControlledFragmentHandler controlled-handler 10)
                          ))]
       (try
        (Thread/sleep 10000)
        (println @sent-messages)
        (println @read-messages)
        
        (finally
         (future-cancel offer-fut)
         (future-cancel receive-fut)))
       



       )
     (finally
      (.close media-driver)))))
