(ns frigit.dump-metadata
  (:require [clojure.string :as s]
            [frigit.core :as frigit])
  (:import [clojure.lang MapEntry]
           [java.util Arrays]))

;; tree, parent*, author, committer \n\n
;; author/committer: name <email> 1422998641 +0000
(defn ^String parse-commit [^bytes b]
  (let [s (String. b)
        end-tree-idx (.indexOf s (int \newline))
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

(defn ^String parse-tree
  [^bytes b]
  (let [acc (transient [])
        space (volatile! 0)
        null (volatile! 0)
        sz (volatile! 0)]
    (while (< @space (alength b))
      (vreset! sz @space)
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


(defn dump-metadata [otype bytes unpack-size]
  (case otype
    :obj_commit (parse-commit @bytes)
    :obj_tree (parse-tree @bytes)
    (name otype)))

(comment

  (def groot2 "/Users/abrooks/.voom-repos/Z2l0QGdpdGh1Yi5jb206amR1ZXkvZWZmZWN0cy5naXQ=/.git/")
  (def groot "/Users/abrooks/repos/lonocore/.git/")

  (time (do (frigit/walk-git-db dump-metadata groot) nil))

  )
