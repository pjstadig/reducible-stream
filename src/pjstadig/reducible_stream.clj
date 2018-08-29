(ns pjstadig.reducible-stream
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io])
  (:import
   (java.io Closeable EOFException PushbackReader)))

(defn- decode!*
  [r rf v f e?]
  (if (e? v)
    r
    (let [r (rf r v)]
      (if (reduced? r)
        r
        (recur r rf (f) f e?)))))

(defn- finalize
  [r rf o]
  (let [r (if (and (not (reduced? r))
                   (contains? o :eof))
            (rf r (:eof o))
            r)]
    (if (reduced? r)
      @r
      r)))

(defn decode!
  "Creates a reducible, seqable object that will decode (using the decoder
  function) a stream created by calling the specified open function (or
  clojure.java.io/input-stream if open is not specified). This object will
  manage the stream (i.e. it will close it when the object has been
  reduced/seq'ed).

  If this object is seq'ed it will entirely consume the stream, fully realize in
  memory the decoded sequence, closed close the stream, then return the
  sequence.

  If this object is reduced it will entirely consume the stream, fully realize
  in memory the decoded/reduced sequence, close the stream, then return the
  sequence.

  However, if a transducer is applied to this object it will only consume as
  much of the stream as is necessary, then it will close the stream before
  terminating the reduction early.

  An optional open function can be specified.  It will take the stream and the
  returned object will be given to the decoder function.

  If the open function is not specified, then clojure.java.io/input-stream will
  be used.

  An optional close function can be specified.  When the reduction has
  terminated the close function will be called and given the object returned
  from the open function.

  If the close function is not specified and the object returned by the open
  function implements java.io.Closeable, then it will be closed.  Otherwise the
  close function is a no-op."
  (^clojure.lang.IReduce
   [decoder streamable]
   (decode! decoder {} streamable))
  (^clojure.lang.IReduce
   [decoder {:as options :keys [open close]} streamable]
   (let [open (or open io/input-stream)
         close (or close
                   (fn close [stream]
                     (when (instance? Closeable stream)
                       (.close ^Closeable stream))))
         eof? (partial identical? ::eof)]
     (reify
       clojure.lang.IReduce
       (reduce [this rf]
         (io!
          (let [stream (open streamable)
                decode #(decoder stream ::eof)]
            (try
              (let [v (decode)]
                (finalize (if (eof? v)
                            (rf)
                            (decode!* v rf (decode) decode eof?))
                          rf
                          options))
              (finally
                (close stream))))))
       (reduce [this rf init]
         (io!
          (let [stream (open streamable)
                decode #(decoder stream ::eof)]
            (try
              (finalize (decode!* init rf (decode) decode eof?)
                        rf
                        options)
              (finally
                (close stream))))))
       clojure.lang.Seqable
       (seq [this]
         (seq (into [] this)))))))

(defn lines-open
  "Used as the open function for decoding text.  Returns a
  java.io.BufferedReader instance."
  ([streamable]
   (lines-open nil streamable))
  ([encoding streamable]
   (io/reader streamable :encoding (or encoding "UTF-8"))))

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
  (^clojure.lang.IReduce
   [streamable]
   (decode-lines! nil streamable))
  (^clojure.lang.IReduce
   [encoding streamable]
   (decode! lines-decoder {:open (partial lines-open encoding)} streamable)))

(defn edn-open
  "Used as the open function for decoding edn.  Returns a java.io.PushbackReader
  instance for use with clojure.edn/read."
  ([streamable]
   (edn-open nil streamable))
  ([encoding streamable]
   (PushbackReader. (io/reader streamable :encoding (or encoding "UTF-8")))))

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
  (^clojure.lang.IReduce
   [streamable]
   (decode-edn! {} streamable))
  (^clojure.lang.IReduce
   [options streamable]
   (decode! (partial edn-decoder (dissoc options :encoding))
            (cond-> {:open (partial edn-open (:encoding options))}
              (contains? options :eof) (assoc :eof (:eof options)))
            streamable)))

(defn clojure-open
  "Used as the open function for decoding clojure.  Returns a
  java.io.PushbackReader instance for use with clojure.core/read."
  ([streamable]
   (clojure-open nil streamable))
  ([encoding streamable]
   (PushbackReader. (io/reader streamable :encoding (or encoding "UTF-8")))))

(defn clojure-decoder
  "Decodes one item from reader returning eof if the end of the reader is
  reached, and passes options along to clojure.core/read."
  ([reader eof]
   (clojure-decoder {} reader eof))
  ([options reader eof]
   (read (assoc options :eof eof) reader)))

(defn decode-clojure!
  "Decodes a stream of clojure data, the :encoding option will be passed to
  clojure-open, and all other options (except :read-eval and :data-readers) are
  passed along to clojure.core/read.  If the :encoding option is not specified,
  it will default to \"UTF-8\".

  If the :read-eval or :data-readers options are specified, they will be used to
  establish bindings for the duration of the reduction.  If either (or both) is
  not specified, it's value will be inherited from the bindings in place in the
  scope the reduction happens (which is not necessarily the same as the scope in
  which decode-clojure! is called)."
  (^clojure.lang.IReduce
   [streamable]
   (decode-clojure! {} streamable))
  (^clojure.lang.IReduce
   [options streamable]
   (let [bindings (select-keys options [:read-eval :data-readers])
         options (dissoc options :read-eval :data-readers)
         r (decode! (partial clojure-decoder (dissoc options :encoding))
                    (cond-> {:open (partial clojure-open (:encoding options))}
                      (contains? options :eof) (assoc :eof (:eof options)))
                    streamable)]
     (reify
       clojure.lang.IReduce
       (reduce [this rf]
         (binding [*read-eval* (if (contains? bindings :read-eval)
                                 (:read-eval bindings)
                                 *read-eval*)
                   *data-readers* (if (contains? bindings :data-readers)
                                    (:data-readers bindings)
                                    *data-readers*)]
           (.reduce r rf)))
       (reduce [this rf init]
         (binding [*read-eval* (if (contains? bindings :read-eval)
                                 (:read-eval bindings)
                                 *read-eval*)
                   *data-readers* (if (contains? bindings :data-readers)
                                    (:data-readers bindings)
                                    *data-readers*)]
           (.reduce r rf init)))))))

(defn- transit-enabled‽
  []
  (require 'cognitect.transit))

(defn transit-open
  "Used as the open function for decoding transit.  Passes along options to
  cognitect.transit/reader, and returns a transit reader for use with
  cognitect.transit/read."
  [type options streamable]
  (transit-enabled‽)
  (let [reader (ns-resolve 'cognitect.transit 'reader)]
    (reader (io/input-stream streamable) type options)))

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
  (^clojure.lang.IReduce
   [type streamable]
   (decode-transit! type {} streamable))
  (^clojure.lang.IReduce
   [type options streamable]
   (transit-enabled‽)
   (let [read (ns-resolve 'cognitect.transit 'read)]
     (decode! (partial transit-decoder read)
              {:open (partial transit-open type options)}
              streamable))))

(defn csv-open
  "Used as the open function for decoding CSV.  Returns a PushbackReader
  instance.  encoding is passed to java.io/reader.  If encoding is not
  specified, it defaults to \"UTF-8\"."
  ([streamable]
   (csv-open nil streamable))
  ([encoding streamable]
   (PushbackReader. (io/reader streamable :encoding (or encoding "UTF-8")))))

;; -- Copied from clojure.data.csv

(def ^:private ^:const lf
  (long \newline))
(def ^:private ^:const cr
  (long \return))
(def ^:private ^:const
  eof -1)

(defn- read-quoted-cell
  [^PushbackReader reader ^StringBuilder sb sep quote]
  (loop [ch (.read reader)]
    (condp == ch
      quote (let [next-ch (.read reader)]
              (condp == next-ch
                quote (do (.append sb (char quote))
                          (recur (.read reader)))
                sep :sep
                lf  :eol
                cr  (let [next-next-ch (.read reader)]
                      (when (not= next-next-ch lf)
                        (.unread reader next-next-ch))
                      :eol)
                eof :eof
                (throw (Exception. ^String (format "CSV error (unexpected character: %c)" next-ch)))))
      eof (throw (EOFException. "CSV error (unexpected end of file)"))
      (do (.append sb (char ch))
          (recur (.read reader))))))

(defn- read-cell
  [^PushbackReader reader ^StringBuilder sb sep quote]
  (let [first-ch (.read reader)]
    (if (== first-ch quote)
      (read-quoted-cell reader sb sep quote)
      (loop [ch first-ch]
        (condp == ch
          sep :sep
          lf  :eol
          cr (let [next-ch (.read reader)]
               (when (not= next-ch lf)
                 (.unread reader next-ch))
               :eol)
          eof :eof
          (do (.append sb (char ch))
              (recur (.read reader))))))))

(defn- read-record [reader sep quote]
  (loop [record (transient [])]
    (let [cell (StringBuilder.)
          sentinel (read-cell reader cell sep quote)]
      (if (= sentinel :sep)
        (recur (conj! record (str cell)))
        [(persistent! (conj! record (str cell))) sentinel]))))

;; -- End clojure.data.csv

(defn csv-decoder
  "Decodes one record from reader returning eof if the end of the reader is
  reached.  sep and quote are the separator and quote characters, respectively."
  [sep quote reader eof]
  (let [[record sentinel] (read-record reader sep quote)]
    (case sentinel
      :eol record
      :eof eof)))

(defn decode-csv!
  "Decodes a stream of CSV data a record at a time.  Options is a hash map
  containing the following keys:

    :encoding   a charset encoding name, passed to csv-open
    :separator  the char used to separate fields in a record
    :quote      the char used to delineate a quoted field
    :header     a function called with each header value before zipping the
                headers with the record values.  If header returns nil, then
                that column will be removed from the resulting map.  If
                specified, the first record will be treated as a header.  If not
                specified, the records are returned as vectors of strings,
                instead of maps"
  (^clojure.lang.IReduce
   [streamable]
   (decode-csv! {} streamable))
  (^clojure.lang.IReduce
   [options streamable]
   (let [sep (or (:separator options) \,)
         quote (or (:quote options) \")
         header (:header options)]
     (cond->> (decode! (partial csv-decoder (int sep) (int quote))
                       {:open (partial csv-open (:encoding options))}
                       streamable)
       header (eduction
               (fn ->map [rf]
                 (let [headers (volatile! nil)]
                   (fn
                     ([] (rf))
                     ([result] (rf result))
                     ([result input]
                      (if-let [headers @headers]
                        (rf result
                            (loop [m (transient {})
                                   headers (seq headers)
                                   input (seq input)]
                              (if (and headers input)
                                (let [k (header (first headers))]
                                  (recur (if k
                                           (assoc! m k (first input))
                                           m)
                                         (next headers)
                                         (next input)))
                                (persistent! m))))
                        (do (vreset! headers input)
                            result)))))))))))
