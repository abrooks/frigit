(ns frigit.core
  (:require [nio.core :as nio]
            [clojure.string :as s])
  (:import [java.io File RandomAccessFile]
           [java.util Arrays]
           [java.util.zip Inflater]
           [java.nio ByteBuffer]
           [java.nio.file Files Paths Path]
           [java.nio.channels.FileChannel]
           [java.nio.charset StandardCharsets]))

;; http://schacon.github.io/gitbook/7_the_packfile.html
;; http://schacon.github.io/gitbook/7_how_git_stores_objects.html
;; https://github.com/git/git/blob/master/Documentation/technical/pack-format.txt

(def idx-magic-v2+ (byte-array (map int "\377tOc")))
(def idx-ver-v2 (byte-array [0 0 0 2]))

(defn check-idx-v2-hdr! [mmchannel]
  (let [magic-bytes (byte-array 4)
        version-bytes (byte-array 4)]
    (doto mmchannel
      (.get magic-bytes 0 4)
      (.get version-bytes 0 4))
    (and (Arrays/equals idx-magic-v2+ magic-bytes)
         (Arrays/equals idx-ver-v2 version-bytes))))

(defn chan->mmchannel [rchan]
  (.map rchan java.nio.channels.FileChannel$MapMode/READ_ONLY 0 (.size rchan)))

(def object-types #{"tree" "blob" "commit" "tag"})
(def object-peek-bytes (count (str "commit " Integer/MAX_VALUE (char 0))))

;; On a large repo which takes 7-8.5s to load, about 1s of that is decompressing commits & trees
(defn unpack-data [bytes size]
  (let [buf (byte-array size)
        infl (Inflater.)]
    (doto infl
      (.setInput bytes)
      (.inflate buf)
      (.end))
    buf))

(defn unpack-object [bytes size]
  (let [buf (unpack-data bytes size)]
    (zipmap [:header :body]
            (s/split (String. buf) (re-pattern (str (char 0))) 2))))

(defn parse-object-hdr [hdr]
  (let [[otype bsize-str] (s/split hdr #" " 2)
        bsize (Integer/parseInt bsize-str)]
    {:type otype
     :size bsize}))

(defn idx-read-entries! [mm num]
  (let [bytes (byte-array 4)
        buf (ByteBuffer/allocate (/ Integer/SIZE 8))]
    (doall
     (for [n (range num)]
       (do
         (.get mm bytes 0 4)
         (doto buf
           (.rewind)
           (.put bytes)
           (.flip))
         (.getInt buf))))))

(defn idx-read-shas! [mm count]
  ;; Extra leading zero byte to keep unsigned
  (let [bytes (byte-array 21)]
    (doall
     (for [n (range count)]
       (do
         (.get mm bytes 1 20)
         (.toString (java.math.BigInteger. bytes) 16))))))

"
NOTES on idx:
- fan-out entries are cumulative so the last entry has total count of objects
- object count from last fan-out is needed to walk v2 chunks
- idx is needed to know sha and offset of each object
"
(defn read-idx [path]
  (let [rchan (nio/readable-channel path)
        mm (chan->mmchannel rchan)
        _ (assert (check-idx-v2-hdr! mm))
        ;; skip to last fan-out entry
        _ (.position mm (+ (* 4 (dec 256)) (.position mm)))
        fan-out (idx-read-entries! mm 1)
        obj-count (last fan-out)
        shas (idx-read-shas! mm obj-count)
        ;; Skipping past CRCs for now
        _ (.position mm (+ (* 4 obj-count) (.position mm)))
        #_#_crcs (idx-read-entries! mm obj-count)
        offs (idx-read-entries! mm obj-count)
        #_"We're not handling extended offsets or trailer checksums"]
    (apply sorted-map (mapcat list offs shas))))

(def pack-hdr-type
  {2r001 :obj_commit
   2r010 :obj_tree
   2r011 :obj_blob
   2r100 :obj_tag
   2r110 :obj_ofs_delta
   2r111 :obj_ref_delta})

(defn read-pack-entry!
  [mm [pos sha] pack-entry-size]
  (.position mm pos)
  (let [hdr (.get mm)
        otype (-> hdr
                  (bit-shift-right 4)
                  (bit-and 2r111)
                  (pack-hdr-type))
        [sz-bytes unpack-size] (loop [last hdr n 0 sz (bit-and 2r00001111 hdr)]
                                 (if (zero? (bit-and 2r10000000 last))
                                   [(inc n) sz]
                                   (let [next (.get mm)
                                         val (-> next
                                                 (bit-and 2r01111111)
                                                 (bit-shift-left (+ 4 (* 7 n))))]
                                     (recur next (inc n) (bit-or val sz)))))
        pack-size (- pack-entry-size sz-bytes)
        bytes (byte-array pack-size)
        _ (.get mm bytes 0 pack-size)
        data (if (#{:obj_commit :obj_tree} otype)
               (unpack-data bytes unpack-size)
               (name otype))]
    [sha  {:type otype :s pack-size :u unpack-size :d (String. data)}]))

(defn read-pack [pack-path]
  (let [idx-path (.replace pack-path ".pack" ".idx")
        idx-data (read-idx idx-path)
        pack-size (.length (File. pack-path))
        pack-objs-end (- pack-size 20)
        rchan (nio/readable-channel pack-path)
        mm (chan->mmchannel rchan)
        sizes (map (fn [[a b]]
                     (- b a))
                   (partition 2 1 [pack-objs-end] (keys idx-data)))
        datas (doall
               (map (partial read-pack-entry! mm)
                    idx-data
                    sizes))]
    (into {} datas)))

(defn load-git-meta
  "Loads all metadata objects (tags/commits/trees - not blobs or deltas)
  Returns map of shas of maps with :type and appropriate params"
  [path]
  (let [d (File. (str path "/objects"))
        loose-objects (for [sha-dir (.listFiles d)
                            :when (not (#{"pack" "info"} (.getName sha-dir)))
                            sha-file (.listFiles sha-dir)
                            :let [sha (str (.getName sha-dir) (.getName sha-file))
                                  bytes (Files/readAllBytes (.toPath sha-file))]]
                        [sha (parse-object-hdr (:header (unpack-object bytes object-peek-bytes)))])
        dp (File. (str path "/objects/pack"))
        packs (for [f (.listFiles dp)
                    :when (re-find #"\.pack$" (.getName f))]
                (.getCanonicalPath f))]
    (apply merge
           (into {} loose-objects)
           (map read-pack packs))))

(comment

  ;; Path structure
"
.git/objects/75
.git/objects/75/04334c6d4970d4fd861555b1dfece1a0aea9e2
.git/objects/7d
.git/objects/7d/eb805c9ee94c601b2a1d6184945cb2cf05e334
.git/objects/aa
.git/objects/aa/f723792d4d25d1904b3f44266e208d51880aee
.git/objects/info
.git/objects/pack
.git/objects/pack/pack-a0e12de24d0a64d98d3f4f78fbcc820c79a721f8.idx
.git/objects/pack/pack-a0e12de24d0a64d98d3f4f78fbcc820c79a721f8.pack
"

  (def groot2 "/Users/abrooks/.voom-repos/Z2l0QGdpdGh1Yi5jb206amR1ZXkvZWZmZWN0cy5naXQ=/.git/")
  (def groot "/Users/abrooks/repos/lonocore/.git/")

  (time (do (load-git-meta groot) nil))



  )

;; large repo stats
;; ~8 seconds total read
;; --
;; 1 second of gzip inflation
;; ~1.8 seconds of reading average of 61byte chunks
;; ~3 seconds of byte-array from collection
;; 90% of the time is idx+pax (idx = 33%, pack = 66% of the 90%)
