(ns flights.core
  (:require
    [flights.queries :as queries]
    [flights.db :as db]
    [flights.import :as import]))

(defn run []
  (db/use! :datascript)
  (db/init!)
  (import/import!)
  (queries/airports-within-bounds-but-not-schengen))
