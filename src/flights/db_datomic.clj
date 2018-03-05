(ns flights.db-datomic
  (:require
    [datomic.api :as d]
    [flights.schema :refer [schema]]))

(def uri "datomic:mem:flights")

(defonce conn (atom nil))

(defn init! []
  (d/create-database uri)
  (reset! conn (d/connect uri))
  (d/transact @conn schema))

(defn transact! [txs]
  (d/transact @conn txs))

(defn query [query & args]
  (apply d/q query (d/db @conn) args))
