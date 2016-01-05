(ns frigit.core
  (:require [nio.core :as nio]
            [clojure.string :as s])
  (:import [java.io File RandomAccessFile]
           [java.util Arrays]
           [java.util.zip Inflater]
           [java.nio ByteBuffer DirectByteBufferR]
           [java.nio.file Files Paths Path]
           [java.nio.channels.FileChannel]))

;; http://schacon.github.io/gitbook/7_the_packfile.html
;; http://schacon.github.io/gitbook/7_how_git_stores_objects.html
;; https://git-scm.com/book/en/v2/Git-Internals-Git-Objects
;; https://github.com/git/git/blob/master/Documentation/technical/pack-format.txt

;; This code is very sensitive to reflection so
(set! *warn-on-reflection* true)

(def idx-magic-v2+ (byte-array (map int "\377tOc")))
(def idx-ver-v2 (byte-array [0 0 0 2]))

(defn check-idx-v2-hdr! [^DirectByteBufferR mmchannel]
  (let [magic-bytes (byte-array 4)
        version-bytes (byte-array 4)]
    (doto mmchannel
      (.get magic-bytes 0 4)
      (.get version-bytes 0 4))
    (and (Arrays/equals ^bytes idx-magic-v2+ magic-bytes)
         (Arrays/equals ^bytes idx-ver-v2 version-bytes))))

(defn chan->mmchannel [^sun.nio.ch.FileChannelImpl rchan]
  (.map rchan java.nio.channels.FileChannel$MapMode/READ_ONLY 0 (.size rchan)))

;; Longest object header we expect to see.
(def object-peek-bytes (count (str "commit " Long/MAX_VALUE (char 0))))

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
  (let [rchan (nio/readable-channel path)
        ^DirectByteBufferR mm (chan->mmchannel rchan)
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

(defn get-pack-unpack-size
  [^DirectByteBufferR mm hdr pack-entry-size]
  (let [[sz-bytes unpack-size]
        , (loop [last hdr, n 0, sz (bit-and 2r00001111 hdr)]
            (if (zero? (bit-and 2r10000000 last))
              [(inc n) sz]
              (let [next (.get mm)
                    val (-> next
                            (bit-and 2r01111111)
                            (bit-shift-left (+ 4 (* 7 n))))]
                (recur next (inc n) (bit-or val sz)))))
        pack-size (- pack-entry-size sz-bytes)]
    [pack-size unpack-size]))

(defn read-pack-entry!
  [handle-obj-fn ^DirectByteBufferR mm [pos sha] pack-entry-size]
  (.position mm pos)
  (let [hdr (.get mm)
        otype (-> hdr
                  (bit-shift-right 4)
                  (bit-and 2r111)
                  (pack-hdr-type))
        [pack-size unpack-size] (get-pack-unpack-size mm hdr pack-entry-size)
        bytes (byte-array pack-size)
        _ (.get mm bytes 0 pack-size)
        data (handle-obj-fn otype bytes unpack-size)]
    [sha  {:type otype :size unpack-size :data data}]))

(defn read-pack-objs
  [handle-obj-fn ^String pack-path]
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
               (map (partial read-pack-entry! handle-obj-fn mm)
                    idx-data
                    sizes))]
    (into {} datas)))

(defn unzip-data [^bytes bytes size]
  (let [buf (byte-array size)
        ;; Re-creating this costs more than 100ms on large repo of ~95K objects
        ;; ... can be reused via .reset but needs to be done thread-safely
        infl (Inflater.)]
    (doto ^Inflater infl
      (.setInput bytes)
      (.inflate buf)
      (.end))
    buf))

(defn unpack-object [bytes size]
  (let [^bytes buf (unzip-data bytes size)]
    (zipmap [:header :body]
            (s/split (String. buf) (re-pattern (str (char 0))) 2))))

(defn parse-object-hdr [^String hdr]
  (let [[otype bsize-str] (s/split hdr #" " 2)
        bsize (Integer/parseInt bsize-str)]
    {:type otype
     :size bsize}))

(defn load-git-loose-objs
  [^String path]
  (doall
   (for [^File sha-dir (-> path (str "/objects") File. .listFiles)
         :when (not (#{"pack" "info"} (.getName sha-dir)))
         ^File sha-file (.listFiles sha-dir)
         :let [sha (str (.getName sha-dir) (.getName sha-file))
               bytes (Files/readAllBytes (.toPath sha-file))]]
     [sha (parse-object-hdr (:header (unpack-object bytes object-peek-bytes)))])))

(defn git-pack-files
  [^String path]
  (doall
   (for [^File f (-> path (str "/objects/pack") File. .listFiles)
         :when (re-find #"\.pack$" (.getName f))]
     (.getCanonicalPath f))))

(defn load-git-meta
  "Loads all metadata objects (tags/commits/trees - not blobs or deltas)
  Returns map of shas of maps with :type and appropriate params"
  [handle-obj-fn path]
  (let [loose-objects (load-git-loose-objs path)
        packs (git-pack-files path)]
    (doall (apply concat loose-objects (map (partial read-pack-objs handle-obj-fn) packs)))))

;; ======================================================================
;; consumer defined
;; ======================================================================

(defn parse-commit [^String s]
  (let [lines (s/split-lines s)
        tree (second (s/split (first lines) #" "))
        parents (->> (rest lines)
                     (filter #(.startsWith ^String % "parent"))
                     (map #(second (s/split % #" "))) )]
    (str  "tree:" tree " parents:" parents)))

(def parent-filtering (filter #(.startsWith ^String % "parent")))
(def value-keeping (map #(second (s/split % #" "))))

(defn parse-commit2 [^String s]
  (let [lines (s/split-lines s)
        tree (second (s/split (first lines) #" "))
        parents (into [] (comp parent-filtering value-keeping) (rest lines))]
    (str  "tree:" tree " parents:" parents)))

;; tree, parent*, author, committer \n\n
;; author/committer: name <email> 1422998641 +0000
(defn ^String parse-commit3 [^String s]
  (let [end-tree-idx (.indexOf s (int \newline))
        ^String tree-hdr (.substring s 0 end-tree-idx)
        end-tree-hdr-idx (.indexOf tree-hdr (int \space))
        tree (.substring tree-hdr (inc end-tree-hdr-idx))
        parents (transient [])]
    (loop [s (.substring s (inc end-tree-idx))]
      (when (.startsWith s "parent ")
        (let [parent-end-idx (.indexOf s (int \newline))]
          (conj! parents (.substring s 0 parent-end-idx))
          (recur (.substring s (inc parent-end-idx))))))
    (persistent! parents)
    ;; TODO NEED CTIME
    (str  "tree:" tree " parents:" parents)))

;; mode \space filename \0 <20-bytes-of-sha>
(defn ^String parse-tree2
  [^String s]
  (prn :count (count s))
  (let [space-pos (.indexOf ^String s (int \space))
        mode (.substring ^String s 0 space-pos)
        s (.substring ^String s (inc space-pos))
        null-pos (.indexOf ^String s 0)
        fname (.substring ^String s 0 null-pos)
        s (.substring ^String s (inc null-pos))
        _ (prn :last fname (count s))
        sha-bytes (.substring ^String s 0 20)
        s (.substring ^String s 20)]
    (prn mode fname (count sha-bytes) (.toString (java.math.BigInteger. (.getBytes sha-bytes)) 16))
    (when (pos? (count s))
      (recur s))))

(defn ^String parse-tree
  [^bytes b]
  (let [acc (transient [])
        space (volatile! 0)
        null (volatile! 0)]
    (while (< @space (alength b))
      (loop [#_space]
        (when (not (= (int \space) (aget b @space)))
          (vswap! space inc)
          (recur)))
      (vreset! null @space)
      (loop [#_null]
        (when (not (= 0 (aget b @null)))
          (vswap! null inc)
          (recur)))
      (vswap! space inc)
      (conj! acc (MapEntry.
                  (String. (Arrays/copyOfRange b (int @space) (int @null)))
                  (.toString (java.math.BigInteger. (Arrays/copyOfRange b (int @null) (int (+ @null 20)))) 16)))
      (vreset! space (+ @null (inc 20))))
    (persistent! acc)))

(defn dispatch-obj-type [otype bytes unpack-size]
  (case otype
    :obj_commit (unzip-data bytes unpack-size) #_(parse-commit3 (String. ^bytes (unzip-data bytes unpack-size)))
    :obj_tree (unzip-data bytes unpack-size) #_(parse-tree (unzip-data bytes unpack-size))
    (name otype)))

(comment

  (def groot2 "/Users/abrooks/.voom-repos/Z2l0QGdpdGh1Yi5jb206amR1ZXkvZWZmZWN0cy5naXQ=/.git/")
  (def groot "/Users/abrooks/repos/lonocore/.git/")

  (time (do (load-git-meta dispatch-obj-type groot) nil))

  )
