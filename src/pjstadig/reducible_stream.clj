(ns pjstadig.reducible-stream
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io])
  (:import
   (java.io Closeable PushbackReader)))

(defn decode!
  "Creates a reducible, seqable object that will decode using the decoder
  funcion the stream that results from giving the specified stream to
  clojure.java.io/input-stream. This object will manage the stream (i.e. it will
  close it when the object has been reduced/seq'ed).

  If this object is seq'ed it will entirely consume the stream, fully realize in
  memory the decoded sequence, closed close the stream, then return the
  sequence.

  If this object is reduced it will entirely consume the stream, fully realize
  in memory the decoded/reduced sequence, close the stream, then return the
  sequence.

  However, if a transducer is applied to this object it will only consume as
  much of the stream as is necessary, then it will close the stream before
  terminating the reduction early.

  An optional open function can be specified, it will take the stream and the
  value that is returned will be given to the decoder function.

  An optional close function can be specified, before the stream is closed the
  close function will be called and given the value that was returned from
  open."
  ([decoder stream]
   (decode! decoder {} stream))
  ([decoder {:keys [open close] :as options} stream]
   (io!
    (reify
      clojure.lang.IReduce
      (reduce [this f]
        (.reduce this f (f)))
      clojure.lang.IReduceInit
      (reduce [this f init]
        (io!
         (with-open [stream (io/input-stream stream)]
           (let [stream (cond-> stream
                          open (open))]
             (try
               (->> (repeatedly #(decoder stream ::eof))
                    (take-while (complement #{::eof}))
                    (reduce f init))
               (finally
                 (if close
                   (close stream)
                   (if (instance? Closeable stream)
                     (.close ^Closeable stream)))))))))
      clojure.lang.Seqable
      (seq [this]
        (seq (into [] this)))
      clojure.lang.Counted
      (count [this]
        (count (seq this)))))))

(defn lines-open
  "Used as the open function for decoding text.  Returns a
  java.io.BufferedReader instance."
  ([stream]
   (lines-open nil stream))
  ([encoding stream]
   (io/reader stream :encoding (or encoding "UTF-8"))))

(defn lines-decoder
  "Decodes one line of text from reader returning eof if the end of the reader
  is reached."
  [^java.io.BufferedReader reader eof]
  (if-let [line (.readLine reader)]
    line
    eof))

(defn decode-lines!
  "Decodes a stream of text data line-by-line, the encoding option will be
  passed to lines-open.  If the :encoding option is not specified, it will
  default to \"UTF-8\"."
  ([stream]
   (decode-lines! nil stream))
  ([encoding stream]
   (decode! lines-decoder {:open (partial lines-open encoding)} stream)))

(defn edn-open
  "Used as the open function for decoding edn.  Returns a java.io.PushbackReader
  instance for use with clojure.edn/read."
  ([stream]
   (edn-open nil stream))
  ([encoding stream]
   (PushbackReader. (io/reader stream :encoding (or encoding "UTF-8")))))

(defn edn-decoder
  "Decodes one item from reader returning eof if the end of the reader is
  reached, and passes options along to clojure.edn/read."
  ([reader eof]
   (edn-decoder {} reader eof))
  ([options reader eof]
   (edn/read (assoc options :eof eof) reader)))

(defn decode-edn!
  "Decodes a stream of edn data, the :encoding option will be passed to
  edn-open, and all other options are passed along to clojure.edn/read.  If
  the :encoding option is not specified, it will default to \"UTF-8\"."
  ([stream]
   (decode-edn! {} stream))
  ([options stream]
   (cond-> (decode! (partial edn-decoder (dissoc options :encoding))
                    {:open (partial edn-open (:encoding options))}
                    stream)
     (contains? options :eof) (concat [(:eof options)]))))

(defn clojure-open
  "Used as the open function for decoding clojure.  Returns a
  java.io.PushbackReader instance for use with clojure.core/read."
  ([stream]
   (clojure-open nil stream))
  ([encoding stream]
   (PushbackReader. (io/reader stream :encoding (or encoding "UTF-8")))))

(defn clojure-decoder
  "Decodes one item from reader returning eof if the end of the reader is
  reached, and passes options along to clojure.core/read."
  ([reader eof]
   (clojure-decoder {} reader eof))
  ([options reader eof]
   (read (assoc options :eof eof) reader)))

(defn decode-clojure!
  "Decodes a stream of clojure data, the :encoding option will be passed to
  clojure-open, and all other options are passed along to clojure.core/read.  If
  the :encoding option is not specified, it will default to \"UTF-8\"."
  ([stream]
   (decode-clojure! {} stream))
  ([options stream]
   (cond-> (decode! (bound-fn* (partial clojure-decoder (dissoc options :encoding)))
                    {:open (partial clojure-open (:encoding options))}
                    stream)
     (contains? options :eof) (concat [(:eof options)]))))

(defn- transit-enabled‽
  []
  (require 'cognitect.transit))

(defn transit-open
  "Used as the open function for decoding transit.  Passes along options to
  cognitect.transit/reader, and returns a transit reader for use with
  cognitect.transit/read."
  [type options stream]
  (transit-enabled‽)
  (let [reader (ns-resolve 'cognitect.transit 'reader)]
    (reader stream type options)))

(defn transit-decoder
  "Decodes one item from reader returning eof if the end of the reader is
  reached."
  [read reader eof]
  (try
    (read reader)
    (catch RuntimeException e
      (if (instance? java.io.EOFException (.getCause e))
        eof
        (throw e)))))

(defn decode-transit!
  "Decodes a stream of transit data passing options along to
  cognitect.transit/reader."
  ([type stream]
   (decode-transit! type {} stream))
  ([type options stream]
   (transit-enabled‽)
   (let [read (ns-resolve 'cognitect.transit 'read)]
     (decode! (partial transit-decoder read)
              {:open (partial transit-open type options)}
              stream))))
