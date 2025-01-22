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

(def ^:dynamic *sha-db* false)

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

(defn read-shas! [^DirectByteBufferR mm count]
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
        shas (read-shas! mm obj-count)
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

(defn read-size-encoding!
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

;; https://github.com/git/git/blob/efff4a85a4fce58b2aa850c6fbf4d8828329f51d/packfile.c#L1226-L1265
(defn read-offset-encoding!
  "Given an 'initial-byte (already read from mm) and how many 'initial-bits that
   are masked by 'initial-mask of that byte, accumulate from subsequent bytes
   on 'mm and return: [bytes-read, encoded-size]"
  [^DirectByteBufferR mm initial-byte initial-bits initial-mask]
  (loop [prev-byte initial-byte, bytes-read 0, sz (bit-and initial-mask initial-byte)]
      (if (not (zero? (bit-and 2r10000000 prev-byte)))
        (let [nsz (bit-shift-left (inc sz) 7)
              next (.get mm) ; read next byte
              val (bit-and next 2r01111111)]
          (recur next (inc bytes-read) (bit-or nsz val)))
        ;; [bytes read, decoded size]
        [(inc bytes-read), sz])))

(defn apply-copy-delta! [param ^DirectByteBufferR buffer base-bytes out-bytes out-idx]
  (let [ofs (if (zero? (bit-and 2r00000001 param)) 0 (bit-and 0xff (.get buffer)))
        ofs (bit-or ofs (bit-shift-left (if (zero? (bit-and 2r00000010 param)) 0 (bit-and 0xff (.get buffer)))  8))
        ofs (bit-or ofs (bit-shift-left (if (zero? (bit-and 2r00000100 param)) 0 (bit-and 0xff (.get buffer))) 16))
        ofs (bit-or ofs (bit-shift-left (if (zero? (bit-and 2r00001000 param)) 0 (bit-and 0xff (.get buffer))) 24))
        sz  (if (zero? (bit-and 2r00010000 param)) 0 (bit-and 0xff (.get buffer)))
        sz  (bit-or sz  (bit-shift-left (if (zero? (bit-and 2r00100000 param)) 0 (bit-and 0xff (.get buffer)))  8))
        sz  (bit-or sz  (bit-shift-left (if (zero? (bit-and 2r01000000 param)) 0 (bit-and 0xff (.get buffer))) 16))
        sz  (if (zero? sz) 0x10000 sz)]
   (System/arraycopy base-bytes ofs out-bytes @out-idx sz)
   (swap! out-idx #(+ % sz))))

(defn apply-append-delta! [sz ^DirectByteBufferR buffer out-bytes out-idx]
  (.get buffer out-bytes @out-idx sz)
  (swap! out-idx #(+ % sz)))

(defn apply-delta [^bytes delta-bytes sha]
  (let [^DirectByteBufferR buffer (ByteBuffer/allocateDirect (alength delta-bytes))
        _ (.put buffer delta-bytes)
        _ (.flip buffer)
        base-init    (.get buffer)
        [_ base-sz]  (read-size-encoding! buffer base-init  7 2r01111111)
        final-init   (.get buffer)
        [_ final-sz] (read-size-encoding! buffer final-init 7 2r01111111)
        base-entry (*sha-db* sha :MISSING)
        base-bytes (-> base-entry (:bytes :MISSINGB) force)
        base-size  (-> base-entry :osize force)
        ;; TODO Decide if we want to unpack osize for the user
        #_ (assert (= base-size (alength ^bytes base-bytes)))
        out-bytes (byte-array final-sz)
        out-idx (atom 0)]
     (while (< 0 (.remaining buffer))
        (let [instr (.get buffer)
              instr-param (bit-and 2r01111111 instr)]
           (case (bit-and 2r10000000 instr)
              2r10000000 (apply-copy-delta! instr-param buffer base-bytes out-bytes out-idx)
              2r00000000 (apply-append-delta! instr-param buffer out-bytes out-idx))))
     out-bytes))

(defn read-pack-entry!
  [idx ^DirectByteBufferR mm pos sha pack-entry-size]
  (.position mm ^int pos)
  (let [hdr (.get mm) ; get a single byte at this position
        otype (-> hdr
                  (bit-and 2r01110000)
                  (bit-shift-right 4)
                  (pack-hdr-type))
        [hdr-bytes unpack-size] (read-size-encoding! mm hdr 4 2r00001111)
        ;; We need to know sz-bytes to know how much packed data is left (from
        ;; pack-entry-size) after reading the variable length header
        pack-size (- pack-entry-size hdr-bytes)
        here (.position mm)
        delay-info (case otype
                     :obj_ref_delta (delay (.position mm ^int here)
		                                  (let [base-sha (first (read-shas! mm 1))
                                            delta-pos (.position mm)
                                            base-type (-> base-sha (*sha-db* :MISSING) :type force)]
                                        (assert base-sha)
                                        {:type base-type :base-sha base-sha :pos delta-pos}))
                     :obj_ofs_delta (delay (.position mm ^int here)
		                                  (let [ibyte (.get mm)
                                            [n-bytes ofs] (read-offset-encoding! mm ibyte 7 2r01111111)
                                            ofs-pos (- pos ofs)
                                            delta-pos (.position mm)
                                            base-sha (-> ofs-pos idx)
                                            base-type (-> base-sha (*sha-db* :MISSING) :type force)]
                                        (assert base-sha)
		                                    {:type base-type :base-sha base-sha :pos delta-pos}))
                      {:type otype :pos here})
        delay-type (delay (-> delay-info force :type))
        delay-bytes (delay (let [entry-bytes (byte-array pack-size)
                                 {:keys [pos base-sha]} (force delay-info)
                                 _ (.position mm ^int pos)
                                 _ (.get mm entry-bytes 0 pack-size)
                                 unpacked (unzip-data entry-bytes unpack-size)]
                              (if (#{:obj_ref_delta :obj_ofs_delta} otype)
                                (do
                                (apply-delta unpacked base-sha))
                                unpacked)))]
    (transient (hash-map
                 :otype otype
                 :osize unpack-size
                 :type delay-type
                 :bytes delay-bytes))))

(defn read-pack-objs!
  [SHA-DB ^String pack-path]
  (let [idx-path (.replace pack-path ".pack" ".idx")
        idx (read-idx idx-path)
        pack-size (.length (File. pack-path))
        pack-objs-end (- pack-size 20)
        rchan (readable-channel pack-path)
        mm (chan->mmchannel rchan)
        sizes (map (fn [[a b]] (- b a))
                   (partition 2 1 [pack-objs-end] (keys idx)))]
  (doseq [[[pos sha] size] (map list idx sizes)
          :let [obj-map (read-pack-entry! idx mm pos sha size)]]
    (assoc! obj-map :file pack-path)
    (assoc! SHA-DB sha obj-map))))

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

(defn load-git-loose-objs!
  [SHA-DB handle-obj-fn ^String path]
  (doseq [^File sha-dir (-> path (str "/objects") File. .listFiles)
         :let [sha-dir-name (.getName sha-dir)]
         :when (re-matches #"[0-9a-f][0-9a-f]" sha-dir-name)
         ^File sha-file (.listFiles sha-dir)
         :let [sha-file-name (.getName sha-file)
               sha (str sha-dir-name sha-file-name)
               ;; TODO don't need to read all bytes, just enough for the header at first
               bytes (Files/readAllBytes (.toPath sha-file))
               header (:header (unpack-object bytes hdr-peek-bytes))
               {:keys [otype osize]} (parse-object-hdr header)
               delay-bytes (delay (:data (unpack-object bytes (+ osize hdr-peek-bytes))))
               file (str path \/ sha-dir-name \/ sha-file-name)]]
     (assoc! SHA-DB sha
                    (transient (hash-map
                                 :otype otype
                                 :osize osize
                                 :file file
                                 :type otype
                                 :bytes delay-bytes)))))

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
  (let [SHA-DB (transient clojure.lang.PersistentHashMap/EMPTY)
        loose-objects (load-git-loose-objs! SHA-DB handle-obj-fn path)
        packs (git-pack-files path)
        _ (doseq [pack packs]
             (read-pack-objs! SHA-DB pack))
        sha-db (persistent! SHA-DB)]
    (binding [*sha-db* sha-db]
      (doseq [[sha {:keys [type bytes osize] :as m}] sha-db
               :let [forced-type (force type)]]
         (assoc! m :type forced-type)
         (assoc! m :repo-path path :data (handle-obj-fn forced-type bytes osize)))
      (doseq [[sha m] sha-db]
         (dissoc! m :pack-mm :bytes)))
    (zipmap (keys sha-db) (map persistent! (vals sha-db)))))

