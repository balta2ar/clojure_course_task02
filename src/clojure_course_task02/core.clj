(ns clojure-course-task02.core
  (:gen-class))

;;
;; Short implementation without parallelization (for reference)
;; Runs ~5s on /usr with empty regexp
;;
(defn find-files-sequential [file-name path]
  (let [pattern (re-pattern file-name)]
    (->> (file-seq (clojure.java.io/file path))
         (map #(.getName %))
         (filter #(re-find pattern %)))))

;;
;; Parallel version without refs
;; Runs ~64s on /usr with empty regexp
;; Extra 60s are probably due to http://dev.clojure.org/jira/browse/CLJ-124
;;
(defn find-files-parallel [file-name path]
  (let [pattern (re-pattern file-name)]

    (defn filter-files [files]
      (filter #(re-find pattern %) files))

    (defn list-files [path]
      (.listFiles (clojure.java.io/file path)))

    ;; parallel version of mapcat
    (defn pmapcat [f & colls]
      (apply concat (apply pmap f colls)))

    (defn handle-file [obj]
      (.getName obj))

    ;; required because I use mutual recursion
    (declare handle-obj)

    (defn handle-dir [obj]
      (->> (.getPath obj)
           (list-files)
           (pmapcat handle-obj)))

    ;; always return a list of names
    (defn handle-obj [obj]
      (if (.isDirectory obj)
        (conj (handle-dir obj) (.getName obj))
        [(handle-file obj)]))

    (defn find-files-local [path]
      (->> (clojure.java.io/file path)
           (handle-obj)
           (filter-files)))

    (let [result (find-files-local path)]
      ;; (shutdown-agents) ; if I turn it on, I get
                           ; Exception in thread "main" java.util.concurrent.RejectedExecutionException
      result)))

;;
;; Parallel version with refs
;; Runs ~??s on /usr with empty regexp
;;
(defn find-files-parallel-refs [file-name path]
  (let [result-list (ref [])
        threads (ref [])]
    (defn show [& args]
      (println args)
      (flush))
    (defn append [pool operation name]
      (do
        ;(show "adding" name)
        (dosync (alter pool operation name))))
    (defn handle [obj]
      (let [name (.getName obj)]
        ;(show "handling" name)
        (if (.isDirectory obj)
          (do (append result-list conj name)                   ; remember dir name
              (let [objs (.listFiles obj)                      ; all objects of the dir
                    files (filter #(.isFile %) objs)           ; files only
                    dirs (filter #(.isDirectory %) objs)]      ; dirs only
                ;(show objs)
                ;(show files)
                ;(show dirs)
                (map (partial append result-list conj) files)  ; remember files
                (let [futures (map #(future (handle %)) dirs)] ; run dir handlers in parallel
                  (append threads into futures)                ; remember threads
                  (doall futures)                              ; force to execute
          (append result-list conj name)))))))                 ; remember filename
    (handle (clojure.java.io/file path))                       ; handle requested path
    (dosync (map deref @threads))                              ; wait for threads to complete
    ;(show "threads" (count @threads))
    @result-list))

(defn find-files [file-name path]
  "TODO: Implement searching for a file using his name as a regexp."
  (find-files-parallel file-name path))

(defn usage []
  (println "Usage: $ run.sh file_name path"))

(defn -main [file-name path]
  (if (or (nil? file-name)
          (nil? path))
    (usage)
    (do
      (println "Searching for " file-name " in " path "...")
      (dorun (map println (find-files file-name path))))))
