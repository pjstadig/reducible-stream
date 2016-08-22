(ns pjstadig.reducible-stream-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer :all]
   [cognitect.transit :as transit]
   [pjstadig.reducible-stream :refer :all]))

(defn lines-stream
  [encoding & lines]
  (io/input-stream (.getBytes (string/join "\n" lines) encoding)))

(deftest t-decode!-as-reducer
  (let [calls (atom [])
        open (fn [stream]
               (swap! calls conj :open)
               (lines-open stream))
        decoder (fn [stream eof]
                  (swap! calls conj :decoder)
                  (lines-decoder stream eof))
        close (fn [stream]
                (swap! calls conj :close))
        result (->> (lines-stream "UTF-8" "first" "second" "third")
                    (decode! decoder {:open open :close close})
                    (into [] (take 1)))]
    (is (= 1 (count result))
        "should return only one item")
    (is (= [:open :decoder :close] @calls)
        "should consume only one item"))
  (is (= "firstsecondthird"
         (reduce (fn
                   ([] "no-arg")
                   ([a b] (str a b)))
                 (->> (lines-stream "UTF-8" "first" "second" "third")
                      (decode! lines-decoder {:open lines-open}))))
      "should take first item when init is not provided")
  (is (= "no-arg"
         (reduce (fn
                   ([] "no-arg")
                   ([a b] (str a b)))
                 (->> (lines-stream "UTF-8")
                      (decode! lines-decoder {:open lines-open}))))
      "should call no-arg reducer when collection is empty"))

(deftest t-decode!-as-seq
  (let [calls (atom [])
        open (fn [stream]
               (swap! calls conj :open)
               (lines-open stream))
        decoder (fn [stream eof]
                  (swap! calls conj :decoder)
                  (lines-decoder stream eof))
        close (fn [stream]
                (swap! calls conj :close))
        result (->> (lines-stream "UTF-8" "first" "second" "third")
                    (decode! decoder {:open open :close close})
                    (take 1))]
    (is (= 1 (count result))
        "should return only one item")
    (is (= [:open :decoder :decoder :decoder :decoder :close] @calls)
        "should consume all items")))

(deftest t-decode-lines!
  (is (= "昨夜のコンサートは最高でした。"
         (first (decode-lines! "SJIS"
                               (lines-stream "SJIS" "昨夜のコンサートは最高でした。"))))
      "should propagate encoding"))

(defn encoded-edn-stream
  [encoding & objs]
  (io/input-stream (.getBytes (string/join (map pr-str objs)) encoding)))

(defn edn-stream
  [& objs]
  (apply encoded-edn-stream "UTF-8" objs))

(deftest t-decode-edn!
  (is (= 42 (first (decode-edn! {:readers {'foo/bar (fn [v] 42)}}
                                (edn-stream (tagged-literal 'foo/bar {})))))
      "should propagate readers option")
  (is (= 42 (last (decode-edn! {:eof 42} (edn-stream {}))))
      "should propagate eof option")
  (is (= "昨夜のコンサートは最高でした。"
         (first
          (decode-edn! {:encoding "SJIS"}
                       (encoded-edn-stream "SJIS"
                                           "昨夜のコンサートは最高でした。"))))
      "should propagate encoing option"))

(defn encoded-clojure-stream
  [encoding & objs]
  (io/input-stream (.getBytes (string/join (map pr-str objs)) encoding)))

(defn clojure-stream
  [& objs]
  (apply encoded-clojure-stream "UTF-8" objs))

(deftest t-decode-clojure!
  (is (= 42
         (first
          (binding [*data-readers* {'foo/bar (fn [v] 42)}]
            (decode-clojure! (clojure-stream (tagged-literal 'foo/bar {}))))))
      "should propagate readers binding")
  (is (= 42 (last (decode-clojure! {:eof 42} (clojure-stream {}))))
      "should propagate eof option")
  (is (= "昨夜のコンサートは最高でした。"
         (first
          (decode-clojure! {:encoding "SJIS"}
                           (encoded-clojure-stream "SJIS"
                                                   "昨夜のコンサートは最高でした。"))))
      "should propagate encoing option"))

(defrecord SomeNewType [])

(defn transit-stream
  [type & objs]
  (let [baos (java.io.ByteArrayOutputStream.)
        options {:handlers (transit/record-write-handlers SomeNewType)}
        writer (transit/writer baos type options)]
    (doseq [obj objs]
      (transit/write writer obj))
    (io/input-stream (.toByteArray baos))))

(deftest t-decode-transit!
  (is (= {:foo "bar"}
         (->> (transit-stream :json {:foo "bar"})
              (decode-transit! :json)
              (first)))
      "should propagate encoding type")
  (is (= {:foo "bar"}
         (->> (transit-stream :msgpack {:foo "bar"})
              (decode-transit! :msgpack)
              (first)))
      "should propagate encoding type")
  (let [read-handlers {:handlers (transit/record-read-handlers SomeNewType)}]
    (is (= (->SomeNewType)
           (->> (transit-stream :json (->SomeNewType))
                (decode-transit! :json read-handlers)
                (first)))
        "should propagate handlers")))
