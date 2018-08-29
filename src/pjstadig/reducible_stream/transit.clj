(ns pjstadig.reducible-stream.transit
  (:refer-clojure :exclude [read])
  (:require
   [clojure.java.io :as io]
   [cognitect.transit :as t]))

(defprotocol ITransitRead
  (read [this]))

(deftype CloseableTransitReader
    [^java.io.Closeable in tr]
  ITransitRead
  (read [this]
    (t/read tr))
  java.io.Closeable
  (close [this]
    (.close in)))

(defn reader
  [type options stream]
  (let [in (io/input-stream stream)]
    (->CloseableTransitReader in (t/reader in type options))))
