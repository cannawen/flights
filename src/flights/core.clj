(ns flights.core
  (:require
    [clojure.data.csv :as csv]
    [datomic.api :as d]))

(defn routes []
  (->> "data/routes.csv"
    slurp
    csv/read-csv
    (remove (fn [row]
              (some (fn [item] (= item "\\N")) row)))
    (map (fn [row]
           {:route/source-id (Integer/parseInt (row 3))
            :route/source-code (row 2)
            :route/destination-id (Integer/parseInt (row 5))
            :route/destination-code (row 4)}))))

(defn airports []
  (->> "data/airports.csv"
    slurp
    csv/read-csv
    (map (fn [row]
           {:airport/latitude (Float/parseFloat (row 6))
            :airport/longitude (Float/parseFloat (row 7))
            :airport/city (row 2)
            :airport/country (row 3)
            :airport/id (Integer/parseInt (row 0))}))))

(defn read-data [file-name row-transform]
  (->> (str "data/" file-name ".csv")
       slurp
       csv/read-csv
       (map row-transform)))

(defn schengen []
  (read-data
    "schengen"
    (fn [row]
      {:schengen/status true
       :schengen/country (row 0)})))

(declare conn)


(def uri "datomic:free://localhost:4334/flights")

(def schema
  [{:db/id #db/id[:db.part/db]
    :db/ident :airport/latitude
    :db/valueType :db.type/float
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :airport/longitude
    :db/valueType :db.type/float
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :airport/city
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :airport/country
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
    
   {:db/id #db/id[:db.part/db]
    :db/ident :airport/id
    :db/valueType :db.type/integer
    :db/unique :db.unique/identity
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :route/source-id
    :db/valueType :db.type/integer
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :route/source-code
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :route/destination-id
    :db/valueType :db.type/integer
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :route/destination-code
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :schengen/status
    :db/valueType :db.type/boolean
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :schengen/country
    :db/valueType :db.type/boolean
    :db.install/_attribute :db.part/db}])

(defn init-db []
  (d/create-database uri)
  (def conn (d/connect uri)))

(defn import! []
  (println "Adding schengen...")
  (d/transact conn (schengen))
  (println "Adding airports...")
  (d/transact conn (airports))
  (println "Adding routes...")
  (d/transact conn (routes))
  nil)

(defn airports-two-away []
  (d/q '[:find ?city
         :where [?airport :city ?city]
         [?airport :id ?airport-id]
         [?route-2 :destination-id ?airport-id]
         [?route-2 :source-id ?stop-1-airport-id]
         [?route-1 :destination-id ?stop-1-airport-id]
         [?route-1 :source-code "TPE"]]
    (d/db conn)))


(defn airports-within-bounds []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find ?city ?country
           :in $ ?lon-min ?lat-min ?lon-max ?lat-max
           :where [?airport :city ?city]
           [?airport :country ?country]
           [?airport :latitude ?latitude]
           [?airport :longitude ?longitude]
           [(< ?lat-min ?latitude ?lat-max)]
           [(< ?lon-min ?longitude ?lon-max)]]
      (d/db conn)
      lon-min lat-min lon-max lat-max)))

(defn airports-one-away-within-bounds []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find ?city ?country
           :in $ ?lon-min ?lat-min ?lon-max ?lat-max
           :where [?airport :city ?city]
           [?airport :country ?country]
           [?airport :latitude ?latitude]
           [?airport :longitude ?longitude]
           [(< ?lat-min ?latitude ?lat-max)]
           [(< ?lon-min ?longitude ?lon-max)]
           [?airport :id ?airport-id]
           [?route :destination-id ?airport-id]
           [?route :source-code "TPE"]]
      (d/db conn)
      lon-min lat-min lon-max lat-max)))

(defn airports-two-away-within-bounds []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find ?city ?country
           :in $ ?lon-min ?lat-min ?lon-max ?lat-max
           :where [?airport :city ?city]
           [?airport :country ?country]
           [?airport :latitude ?latitude]
           [?airport :longitude ?longitude]
           [(< ?lat-min ?latitude ?lat-max)]
           [(< ?lon-min ?longitude ?lon-max)]
           [?airport :id ?airport-id]
           [?route-2 :destination-id ?airport-id]
           [?route-2 :source-id ?stop-1-airport-id]
           [?route-1 :destination-id ?stop-1-airport-id]
           [?route-1 :source-code "TPE"]]
      (d/db conn)
      lon-min lat-min lon-max lat-max)))

(defn airports-two-away-within-bounds-optimized []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find ?city ?country
           :in $ ?lon-min ?lat-min ?lon-max ?lat-max
           :where
           [?route-1 :source-code "TPE"]
           [?route-1 :destination-id ?stop-1-airport-id]
           [?route-2 :source-id ?stop-1-airport-id]
           [?route-2 :destination-id ?airport-id]
           [?airport :id ?airport-id]
           [?airport :latitude ?latitude]
           [?airport :longitude ?longitude]
           [(< ?lat-min ?latitude ?lat-max)]
           [(< ?lon-min ?longitude ?lon-max)]

           [?airport :country ?country]
           [?airport :city ?city]]





      (d/db conn)
      lon-min lat-min lon-max lat-max)))


(defn airports-two-away-within-schengen-and-bounds []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find ?city ?country
           :in $ ?lon-min ?lat-min ?lon-max ?lat-max
           :where
           [?airport :city ?city]
           [?schengen-country-id :schengen true]
           [?schengen-country-id :country ?country]
           [?country-id :country ?country]
           [?airport :country ?country]
           [?airport :latitude ?latitude]
           [?airport :longitude ?longitude]
           [(< ?lat-min ?latitude ?lat-max)]
           [(< ?lon-min ?longitude ?lon-max)]
           [?airport :id ?airport-id]
           [?route-2 :destination-id ?airport-id]
           [?route-2 :source-id ?stop-1-airport-id]
           [?route-1 :destination-id ?stop-1-airport-id]
           [?route-1 :source-code "TPE"]]
           ;[(get-else $ ?country-id :schengen false) ?schengen-status]
           ;[(= ?schengen-status false)]]
      (d/db conn)
      lon-min lat-min lon-max lat-max)))



(def run airports-two-away-within-schengen-and-bounds)
