(ns pjstadig.reducible-stream.transit
  (:refer-clojure :exclude [read])
  (:require
   [clojure.java.io :as io]
   [cognitect.transit :as t]))

(defprotocol ITransitRead
  (read [this]))

(deftype CloseableTransitReader
    [^java.io.Closeable stream transit-reader]
  ITransitRead
  (read [_]
    (t/read transit-reader))
  java.io.Closeable
  (close [_]
    (.close stream)))

(defn reader
  [type options streamable]
  (let [stream (io/input-stream streamable)]
    (->CloseableTransitReader stream (t/reader stream type options))))
