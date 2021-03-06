(defproject pjstadig/reducible-stream "0.2.0-SNAPSHOT"
  :description "Reducible stream for decoding data."
  :url "https://github.com/pjstadig/reducible-stream"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["releases" :clojars]]
  :dependencies [[org.clojure/clojure "1.8.0"]]
  :profiles {:dev {:dependencies [[com.cognitect/transit-clj "0.8.288"]]}})
