(ns frigit.dump-metadata
  (:require [clojure.string :as s]
            [frigit.core :as frigit])
  (:import [clojure.lang MapEntry]
           [java.io File]
           [java.util Arrays]))

(defn parse-author-committer
  "Parse author/committer line into parts"
  [s]
  (let [[role & infos] (s/split s #"[<> ]+")
        [tz epochtime email & nameparts] (reverse infos)
        name (s/join " " (reverse nameparts))]
    {:role role :name name :email email :epochtime epochtime :tz tz}))

;; One per line:
;; tree, parent*, author, committer \n\n description
;; author/committer = name <email> 1422998641 +0000
(defn parse-commit [^bytes b]
  (let [s (String. b)
        lines (s/split-lines s)
        [tree-entry & lines] lines
        _ (assert (.startsWith tree-entry "tree "))
        tree (second (s/split tree-entry #" " 2))
        [parent-entries lines] (split-with #(.startsWith % "parent ") lines)
        parents (mapv #(second (s/split % #" " 2)) parent-entries)
        [author-committer-entries message-entries]
        , (split-with #(or (.startsWith % "author ")
                           (.startsWith % "committer "))
                      lines)
        author-committer (map parse-author-committer author-committer-entries)
        message (s/join "\n" (rest message-entries)) ; rest skips \n\n
        res {:tree tree :parents parents :message message}]
     (reduce #(assoc %1 (keyword (:role %2)) %2) res author-committer)))


(defn parse-tree
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

(defn dump-subdirs [path]
  (let [dirs (->> path File. .listFiles (map #(str % "/.git")))]
    (for [dir dirs]
      (frigit/walk-git-db dump-metadata dir))))

(comment

  (require '[frigit.core :as fc])
  (require '[frigit.dump-metadata :as dm])

  (def groot2 "/Users/abrooks/.voom-repos/Z2l0QGdpdGh1Yi5jb206amR1ZXkvZWZmZWN0cy5naXQ=/.git/")
  (def groot "/Users/abrooks/repos/lonocore/.git/")

  (time (do (frigit/walk-git-db dump-metadata groot) nil))

  )
