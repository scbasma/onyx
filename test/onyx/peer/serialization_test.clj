(ns onyx.peer.serialization-test
   (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
             [taoensso.nippy :as n])
   (:import [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]
            [org.agrona.concurrent UnsafeBuffer]
            [java.nio ByteBuffer]))

(let [decoders #^"[Lclojure.lang.IFn;" (make-array clojure.lang.IFn 30)]
  (aset decoders (MessageEncoder/TEMPLATE_ID) (fn [v] (MessageDecoder.)))
  decoders)

(time 
 (let [encoder ^MessageEncoder (MessageEncoder.)
       buffer (byte-array 900000)
       buf ^UnsafeBuffer (UnsafeBuffer. buffer)]
   (dotimes [v 10000]
     (let [encoder (-> encoder 
                       ;(.blockLength MessageEncoder/BLOCK_LENGTH)
                       ;(.templateId MessageEncoder/TEMPLATE_ID)
                       (.wrap buf 0)
                       (.replicaVersion 5000000000)
                       (.destId 6999))
           seg-count 200
           segments (map (fn [v]
                           {:n v :a "hiseta" :b "esntiarn"})
                         (range seg-count))
           bs ^bytes (messaging-compress {:n 3 :a "isetna" :b "sietna"}) 
           seg-encoder (loop [^MessageEncoder$SegmentsEncoder enc (.segmentsCount encoder seg-count)
                              v (first segments) 
                              vs (rest segments)]
                         (let [;bs ^bytes (messaging-compress v) 
                               cnt ^int (alength bs)]
                           (when v 
                             (recur (.putSegmentBytes (.next enc) bs 0 cnt)
                                    (first vs) 
                                    (rest vs)))))]
       (let [decoder (-> (MessageDecoder.)
                         ;; don't hardcode this thing
                         (.wrap buf 0 MessageDecoder/BLOCK_LENGTH 0))
             replica-version (.replicaVersion decoder)
             dest-id (.destId decoder)
             seg-dec (.segments decoder)
             messages (transient [])]
         (loop [seg-dec seg-dec]
           (if (.hasNext seg-dec)
             ;; can use an unsafe buffer here, but we have no way to read from it with nippy without
             ;; copying to another byte array
             (let [bs (byte-array (.segmentBytesLength seg-dec))]
               (.getSegmentBytes seg-dec bs 0 (.segmentBytesLength seg-dec))
               (conj! messages (messaging-decompress bs))
               (recur (.next seg-dec)))))
         {:replica-version replica-version 
          :dest-id dest-id 
          :segments (persistent! messages)})))))
