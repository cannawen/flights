(ns flights.db-datascript
  (:require
    [datascript.core :as d]))

(defonce conn (atom nil))

(defn init! []
  (reset! conn (d/create-conn {})))

(defn transact! [txs]
  (d/transact! @conn txs))

(defn query [query & args]
  (apply d/q query @@conn args))
