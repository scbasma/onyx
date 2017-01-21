(ns onyx.peer.serialization-test
   (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
             [taoensso.nippy :as n])
   (:import [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]
            [org.agrona.concurrent UnsafeBuffer]
            [java.nio ByteBuffer]))

(def words ["sietna" "itnarinst"])

; (time 
;   (let [encoder (MapEncoder.)
;         buffer (byte-array 900)
;         buf ^UnsafeBuffer (UnsafeBuffer. buffer)]
;     (dotimes [t 1000000] 
;       (let [a "sietna"
;             b " itnarinst"
;             aa (.getBytes a)
;             bb (.getBytes b)
;             encoder (-> encoder 
;                         (.wrap buf 0)
;                         (.putKey aa 0 (alength aa))
;                         (.putValue bb 0 (alength bb)))]
;         (.encodedLength encoder)))))

(let [decoders #^"[Lclojure.lang.IFn;" (make-array clojure.lang.IFn 30)]
  (aset decoders (MessageEncoder/TEMPLATE_ID) (fn [v] (MessageDecoder.)))
  decoders)




            .wrap(directBuffer, bufferOffset)                                                                                                                                                                                                 
            .blockLength(CAR_ENCODER.sbeBlockLength())                                                                                                                                                                                        
            .templateId(CAR_ENCODER.sbeTemplateId())                                                                                                                                                                                          
            .schemaId(CAR_ENCODER.sbeSchemaId())                                                                                                                                                                                              
            .version(CAR_ENCODER.sbeSchemaVersion());                                                                                                                                                                                         


(time 
 (let [encoder ^MessageEncoder (MessageEncoder.)
       buffer (byte-array 900000)
       buf ^UnsafeBuffer (UnsafeBuffer. buffer)]
   ;dotimes [v 1]
   (let [encoder (-> encoder 
                     ;(.blockLength MessageEncoder/BLOCK_LENGTH)
                     ;(.templateId MessageEncoder/TEMPLATE_ID)
                     (.wrap buf 0)
                     (.destId 68830003)
                     (.replicaVersion 5000000000))
         seg-count 20
         seg-encoder (reduce (fn [^MessageEncoder$SegmentsEncoder enc v]
                               (let [bs ^bytes (messaging-compress {:a "hiseta" :b "esntiarn"})
                                     cnt ^int (alength bs)] 
                                 (.putSegmentBytes (.next enc)
                                                   bs
                                                   0 
                                                   cnt)))
                             (.segmentsCount encoder seg-count)
                             (range seg-count))]
     (let [decoder (-> (MessageDecoder.)
                       ;; don't hardcode this thing
                       (.wrap buf 0 MessageDecoder/BLOCK_LENGTH 0))
           ;segments (.segments decoder)
           
           ]
       (let [
             
           byte-array  (.payloadBytesLength decoder)
           byte-array 
           byte-array 
             ])
       ;(.encodedLength decoder)
       ;(.encodedLength encoder)
       ;(messaging-decompress (.segmentBytes (.next seg-dec)))
       (.count seg-dec)
       
       
       ))))

(time (let [encoder ^MessageEncoder (MessageEncoder.)
            buffer (byte-array 900)
            buf ^UnsafeBuffer (UnsafeBuffer. buffer)]
        (dotimes [v 1000000]
          (let [encoder (-> encoder 
                          (.wrap buf 0)
                          (.replicaVersion 50000332)
                          (.destId 68830003)
                          (.replicaVersion 5000000000))
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              bs ^bytes (messaging-compress {:a nil :b nil})
              cnt ^int (alength bs)
              segments-encoder (-> (.segmentsCount encoder 18)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt)
                                   (.putSegmentBytes bs 0 cnt))]
          (.encodedLength encoder)
          ))
  ;(into [] buffer)
  ))

