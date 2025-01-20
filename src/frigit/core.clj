(ns frigit.core
  (:require [clojure.string :as s])
  (:import [java.io File FileFilter RandomAccessFile]
           [java.util Arrays]
           [java.util.zip Inflater]
           [java.nio ByteBuffer DirectByteBufferR]
           [java.nio.file Files Paths Path StandardOpenOption]
           [java.nio.channels FileChannel]))

;; http://schacon.github.io/gitbook/7_the_packfile.html
;; http://schacon.github.io/gitbook/7_how_git_stores_objects.html
;; https://git-scm.com/book/en/v2/Git-Internals-Git-Objects
;; https://github.com/git/git/blob/master/Documentation/gitformat-pack.txt

;; This code is very sensitive to reflection so
(set! *warn-on-reflection* true)

(def idx-magic-v2+ (byte-array (map int "\377tOc")))
(def idx-ver-v2 (byte-array [0 0 0 2]))

;; ---------------------------------------------------------------------
;; Replacement for pjstadig/nio.core functionality:
(defn readable-channel
  "Opens a FileChannel in read-only mode for the given path."
  [^String path]
  (FileChannel/open
    (Paths/get path (into-array String []))
    (into-array java.nio.file.OpenOption [StandardOpenOption/READ])))
;; ---------------------------------------------------------------------

(defn split-byte-array
  "Splits byte-array at first null/0 value"
  [^bytes byte-array]
  ;; This loop will intentionally throw an ArrayIndexOutOfBoundsException exception if no 0 is found.
  (let [^int idx (loop [i 0]
              (if (= 0 (aget byte-array i))
                i
                (recur (inc i))))]
     [(Arrays/copyOf byte-array idx)
      (Arrays/copyOfRange byte-array (inc idx) (alength byte-array))]))

(defn ^byte/1 unzip-data [^byte/1 bytes size]
  (let [buf (byte-array size)
        ;; Re-creating this costs more than 100ms on large repo of ~95K objects
        ;; ... can be reused via .reset but needs to be done thread-safely
        ;; Maybe use: https://github.com/flatland/useful/blob/develop/src/flatland/useful/utils.clj#L192
        ;; Need to find a pmap or similar that can take a passed in or bound threadpool / factory
        infl (Inflater.)
        _ (.setInput infl bytes)
        len (.inflate infl buf)
        _ (.end infl)
        buf (Arrays/copyOf ^bytes buf len)]
    buf))

(defn check-idx-v2-hdr! [^DirectByteBufferR mmchannel]
  (let [magic-bytes (byte-array 4)
        version-bytes (byte-array 4)]
    (doto mmchannel
      (.get magic-bytes 0 4)
      (.get version-bytes 0 4))
    (and (Arrays/equals ^bytes idx-magic-v2+ magic-bytes)
         (Arrays/equals ^bytes idx-ver-v2 version-bytes))))

(defn chan->mmchannel
  "Memory-maps the given FileChannel in READ_ONLY mode."
  [^FileChannel rchan]
  (.map rchan java.nio.channels.FileChannel$MapMode/READ_ONLY 0 (.size rchan)))

;; Longest object header we expect to see.
;; git/git object-file.c defines MAX_HEADER_LEN to 32
(def hdr-peek-bytes 32)

(defn idx-read-entries! [^DirectByteBufferR mm num]
  (let [bytes (byte-array 4)
        buf (ByteBuffer/allocate (/ Integer/SIZE 8))
        acc (transient [])]
    (dotimes [n num]
      (.get mm bytes 0 4)
      (doto buf (.rewind) (.put bytes) (.flip))
      (conj! acc (.getInt buf)))
    (persistent! acc)))

(defn idx-read-shas! [^DirectByteBufferR mm count]
  ;; Extra leading zero byte to keep leading bits unsigned
  (let [bytes (byte-array 21)
        acc (transient [])]
    (dotimes [n count]
      (.get mm bytes 1 20)
      (conj! acc (.toString (java.math.BigInteger. bytes) 16)))
    (persistent! acc)))

;; NOTES on idx:
;; - fan-out entries are cumulative so the last entry has total count of objects
;; - object count from last fan-out is needed to walk v2 chunks
;; - idx is needed to know sha and offset of each object
(defn read-idx [path]
  (let [rchan (readable-channel path)
        ^DirectByteBufferR mm (chan->mmchannel rchan)
        _ (assert (check-idx-v2-hdr! mm))
        ;; skip to last fan-out entry
        _ (.position mm (+ (* 4 (dec 256)) (.position mm)))
        fan-out (idx-read-entries! mm 1)
        obj-count (last fan-out)
        shas (idx-read-shas! mm obj-count)
        ;; Skipping past CRCs for now
        _ (.position mm ^int (+ (* 4 obj-count) (.position mm)))
        #_#_crcs (idx-read-entries! mm obj-count)
        offs (idx-read-entries! mm obj-count)
        #_"We're not handling extended offsets or trailer checksums"]
    (apply sorted-map (mapcat list offs shas))))

(def pack-hdr-type
  "3-bit object types"
  {;_0 2r000 :INVALID
   #_1 2r001 :obj_commit
   #_2 2r010 :obj_tree
   #_3 2r011 :obj_blob
   #_4 2r100 :obj_tag
   ;_5 2r101 :RESERVED
   #_6 2r110 :obj_ofs_delta
   #_7 2r111 :obj_ref_delta})

(defn read-size-offset-encoding!
  "Given an 'initial-byte (already read from mm) and how many 'initial-bits that
   are masked by 'initial-mask of that byte, accumulate from subsequent bytes
   on 'mm and return: [bytes-read, encoded-size]"
  [^DirectByteBufferR mm initial-byte initial-bits initial-mask]
  ;; We could use just initial-bits and compute the initial-mask:
  ;;    (dec (bit-shift-left 2r1 initial-bits))
  ;; but this takes computation and is also less visibly clear as to what's going on.
  ;; Conversely, we could just take the mask and find the position of the
  ;; highest bit set but that also adds needless computation and complexity.
  ;; Better to just be a tad redundant in this case.
  (loop [prev-byte initial-byte, bytes-read 0, sz (bit-and initial-mask initial-byte)]
      (if (not (zero? (bit-and 2r10000000 prev-byte)))
        (let [next (.get mm) ; read next byte
              val (-> next
                      (bit-and 2r01111111)
                      ;; We gain 7 bits of encoded size/offset from each subsequent byte read
                      (bit-shift-left (+ initial-bits (* 7 bytes-read))))]
          (recur next (inc bytes-read) (bit-or val sz)))
        ;; [bytes read, decoded size]
        [(inc bytes-read), sz])))

(defn read-pack-entry!
  [handle-obj-fn ^DirectByteBufferR mm [pos sha] pack-entry-size]
  (.position mm ^int pos)
  (let [hdr (.get mm) ; get a single byte at this position
        otype (-> hdr
                  (bit-and 2r01110000)
                  (bit-shift-right 4)
                  (pack-hdr-type))
        [hdr-bytes unpack-size] (read-size-offset-encoding! mm hdr 4 2r00001111)
        ;; We need to know sz-bytes to know how much packed data is left (from
        ;; pack-entry-size) after reading the variable length header
        pack-size (- pack-entry-size hdr-bytes)
        bytes (byte-array pack-size)
        _ (.get mm bytes 0 pack-size)
        data (handle-obj-fn otype (delay (unzip-data bytes unpack-size)) unpack-size)]
    [sha  {:otype otype :osize unpack-size :loc "PACK" :data data}]))

(defn read-pack-objs
  [handle-obj-fn ^String pack-path]
  (let [idx-path (.replace pack-path ".pack" ".idx")
        idx-data (read-idx idx-path)
        pack-size (.length (File. pack-path))
        pack-objs-end (- pack-size 20)
        rchan (readable-channel pack-path)
        mm (chan->mmchannel rchan)
        sizes (map (fn [[a b]]
                     (- b a))
                   (partition 2 1 [pack-objs-end] (keys idx-data)))
        datas (doall
               (map (partial read-pack-entry! handle-obj-fn mm)
                    idx-data
                    sizes))]
    datas))

;; Loose object format is:
;; - whole file is compressed
;; - uncompressed stream starts with header followed by data
;; - header is: [string_object_type] <SPACE> [string_decimal_length] <NULL>

(defn unpack-object [bytes size]
  (let [^bytes buf (unzip-data bytes size)
        [header data] (split-byte-array buf)]
    {:header (String. ^bytes header) :data data}))

(def loose-hdr-type
  {"commit" :obj_commit
   "tree"   :obj_tree
   "blob"   :obj_blob
   "tag"    :obj_tag})

(defn parse-object-hdr [^String hdr]
  (let [[otype bsize-str] (s/split hdr #" " 2)
        bsize (Long/parseLong bsize-str)]
    {:otype (loose-hdr-type otype)
     :osize bsize}))

(defn load-git-loose-objs
  [handle-obj-fn ^String path]
  (doall
   (for [^File sha-dir (-> path (str "/objects") File. .listFiles)
         :let [sha-dir-name (.getName sha-dir)]
         :when (re-matches #"[0-9a-f][0-9a-f]" sha-dir-name)
         ^File sha-file (.listFiles sha-dir)
         :let [sha-file-name (.getName sha-file)
               sha (str sha-dir-name sha-file-name)
               ;; TODO don't need to read all bytes, just enough for the header at first
               bytes (Files/readAllBytes (.toPath sha-file))
               {:keys [header data]} (unpack-object bytes hdr-peek-bytes)
               {:keys [otype osize]} (parse-object-hdr header)
               data (delay (:data (unpack-object bytes (+ osize hdr-peek-bytes))))
               loc (str path \/ sha-dir-name \/ sha-file-name)]]
     [sha {:otype otype :osize osize :loc loc :data (handle-obj-fn otype data osize)}])))

(defn gen-file-filter [re]
  (reify FileFilter
    (^boolean accept [_ ^File file]
      (boolean (re-find re (.getName file))))))

(def pack-file-filter (-> "\\.pack$" re-pattern gen-file-filter))

(defn git-pack-files
  [^String path]
  (doall
   (for [^File f (-> path (str "/objects/pack") File. (.listFiles ^FileFilter pack-file-filter))]
     (.getCanonicalPath f))))

(defn walk-git-db
  "Loads all metadata objects (tags/commits/trees - not blobs or deltas)
  Returns map of shas of maps with :otype and appropriate params"
  [handle-obj-fn path]
  (let [loose-objects (load-git-loose-objs handle-obj-fn path)
        packs (git-pack-files path)]
    (doall (apply concat loose-objects (map (partial read-pack-objs handle-obj-fn) packs)))))

