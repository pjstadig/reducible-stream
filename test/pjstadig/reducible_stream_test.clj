(ns pjstadig.reducible-stream-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.test :refer :all]
   [cognitect.transit :as transit]
   [pjstadig.reducible-stream :refer :all]))

(defn lines-data
  [encoding & lines]
  (.getBytes (string/join "\n" lines) encoding))

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
        result (->> (lines-data "UTF-8" "first" "second" "third")
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
                 (->> (lines-data "UTF-8" "first" "second" "third")
                      (decode! lines-decoder {:open lines-open}))))
      "should take first item when init is not provided")
  (is (= "no-arg"
         (reduce (fn
                   ([] "no-arg")
                   ([a b] (str a b)))
                 (->> (lines-data "UTF-8")
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
        result (->> (lines-data "UTF-8" "first" "second" "third")
                    (decode! decoder {:open open :close close})
                    (take 1))]
    (is (= 1 (count result))
        "should return only one item")
    (is (= [:open :decoder :decoder :decoder :decoder :close] @calls)
        "should consume all items")))

(deftest t-decode-lines!
  (is (= ["昨夜のコンサートは最高でした。"]
         (into []
               (take 1)
               (decode-lines! "SJIS"
                              (lines-data "SJIS" "昨夜のコンサートは最高でした。"))))
      "should propagate encoding"))

(defn encoded-edn-data
  [encoding & objs]
  (.getBytes (string/join (map pr-str objs)) encoding))

(defn edn-data
  [& objs]
  (apply encoded-edn-data "UTF-8" objs))

(deftest t-decode-edn!
  (is (= [42]
         (into []
               (take 1)
               (decode-edn! {:readers {'foo/bar (fn [v] 42)}}
                            (edn-data (tagged-literal 'foo/bar {})))))
      "should propagate readers option")
  (is (= [42]
         (into []
               (comp (drop 1)
                     (take 1))
               (decode-edn! {:eof 42} (edn-data {}))))
      "should propagate eof option")
  (is (= ["昨夜のコンサートは最高でした。"]
         (into []
               (take 1)
               (decode-edn! {:encoding "SJIS"}
                            (encoded-edn-data "SJIS"
                                              "昨夜のコンサートは最高でした。"))))
      "should propagate encoing option"))

(defn encoded-clojure-data
  [encoding & objs]
  (.getBytes (string/join (map pr-str objs)) encoding))

(defn clojure-data
  [& objs]
  (apply encoded-clojure-data "UTF-8" objs))

(deftest t-decode-clojure!
  (let [r (binding [*data-readers* {'foo/bar (fn [v] 42)}]
            (decode-clojure! (clojure-data (tagged-literal 'foo/bar {}))))]
    (is (= [42]
           (into []
                 (take 1)
                 r))
        "should propagate readers binding"))
  (let [r (binding [*read-eval* false]
            (decode-clojure! (.getBytes "#=(+ 1 2)")))]
    (is (thrown? RuntimeException
                 (binding [*read-eval* true]
                   (into []
                         (take 1)
                         r)))
        "should propagate read-eval binding"))
  (is (= [42]
         (into []
               (comp (drop 1)
                     (take 1))
               (decode-clojure! {:eof 42} (clojure-data {}))))
      "should propagate eof option")
  (is (= ["昨夜のコンサートは最高でした。"]
         (into []
               (take 1)
               (decode-clojure! {:encoding "SJIS"}
                                (encoded-clojure-data "SJIS"
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
    (.toByteArray baos)))

(deftest t-decode-transit!
  (is (= [{:foo "bar"}]
         (->> (transit-stream :json {:foo "bar"})
              (decode-transit! :json)
              (into [] (take 1))))
      "should propagate encoding type")
  (is (= [{:foo "bar"}]
         (->> (transit-stream :msgpack {:foo "bar"})
              (decode-transit! :msgpack)
              (into [] (take 1))))
      "should propagate encoding type")
  (let [read-handlers {:handlers (transit/record-read-handlers SomeNewType)}]
    (is (= [(->SomeNewType)]
           (->> (transit-stream :json (->SomeNewType))
                (decode-transit! :json read-handlers)
                (into [] (take 1))))
        "should propagate handlers")))

(deftest t-decode-csv!
  (let [csv-data (.getBytes "a,b,c\n1,2,3\n4,5,6\n")]
    (is (= [["a" "b" "c"]]
           (->> csv-data
                (decode-csv!)
                (into [] (take 1))))
        "should parse csv")
    (is (= [{"a" "1" "b" "2" "c" "3"}]
           (->> csv-data
                (decode-csv! {:header str})
                (into [] (take 1))))
        "should parse header into maps"))
  (is (= [["a" "b" "c"]]
         (->> (.getBytes "a|b|c\n1|2|3\n4|5|6\n")
              (decode-csv! {:separator \|})
              (into [] (take 1))))
      "should take separator")
  (is (= [["\"a\"" "\"b\"" "\"c\""]]
         (->> (.getBytes "'\"a\"','\"b\"','\"c\"'\n1,2,3\n4,5,6\n")
              (decode-csv! {:quote \'})
              (into [] (take 1))))
      "should take quote"))
