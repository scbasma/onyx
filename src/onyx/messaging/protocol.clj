(ns onyx.messaging.protocol
  (:import ;[baseline SegmentEncoder SegmentDecoder TypeEnum AnyTypeEncoder AnyTypeDecoder BarrierEncoder BarrierDecoder MessageContainerEncoder MessageContainerDecoder]
           [uk.co.real_logic.agrona.concurrent UnsafeBuffer]))

(defn new-buffer []
  (let [ba (byte-array 1000)] 
    (UnsafeBuffer. ba)))

(comment
(def encoder (MessageContainerEncoder.))

(def decoder (MessageContainerDecoder.))

(def buffer (new-buffer))

(def bs (byte-array 3))

(aset bs 0 (byte 1))
(aset bs 1 (byte 2))
(aset bs 2 (byte 3))

(def encoded 
  (-> (.wrap encoder buffer 0)
      (.putPayload bs (int 0) (int (count bs)))
      ;(.serialNumber 3)
      ;(.type TypeEnum/Keyword)
      ;(.fromTaskNum 3)
      ;(.toTaskNums 1 1)
      ;(.toTaskNums 0 5)
      ))

(def length (.encodedLength encoded))


 
 
  (.string (.wrap decoder buffer 0) 0)
  


(def decoded
  (-> (.wrap decoder buffer 0 length 0)
      (.toTaskNums 1))) 
  )
