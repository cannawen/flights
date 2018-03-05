(ns flights.import
  (:require
    [clojure.data.csv :as csv]
    [flights.db :as db]))

(defn- read-data [file-name]
  (->> (str "data/" file-name ".csv")
       slurp
       csv/read-csv))

(defn- schengen []
  (->> (read-data "schengen")
       (map (fn [row]
              (row 0)))
       set))

(defn- countries []
  (let [schengen? (schengen)]
    (->> (read-data "countries")
         (map (fn [row]
                {:country/name (row 0)
                 :country/schengen? (contains? schengen? (row 0))})))))

(defn- airports []
  (->> (read-data "airports")
       (map (fn [row]
              {:airport/latitude (Float/parseFloat (row 6))
               :airport/longitude (Float/parseFloat (row 7))
               :airport/city (row 2)
               :airport/country (row 3)
               :airport/id (Integer/parseInt (row 0))}))))

(defn- routes []
  (->> (read-data "routes")
       (remove (fn [row]
                 (some (fn [item] (= item "\\N")) row)))
       (map (fn [row]
              {:route/source-id (Integer/parseInt (row 3))
               :route/source-code (row 2)
               :route/destination-id (Integer/parseInt (row 5))
               :route/destination-code (row 4)}))))

(defn import! []
  (println "Adding countries...")
  (db/transact! (countries))
  (println "Adding airports...")
  (db/transact! (airports))
  (println "Adding routes...")
  (db/transact! (routes))
  nil)
