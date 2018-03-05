(ns flights.queries
  (:require
    [flights.db :as db]))

(defn- parse-bounds [bounds-string]
  (-> bounds-string
      (clojure.string/split #",")
      (->> (map #(Float/parseFloat %)))))

(def rules
  '[

    [(airport-within-bounds ?lon-min ?lat-min ?lon-max ?lat-max ?airport)
     [?airport :airport/latitude ?latitude]
     [?airport :airport/longitude ?longitude]
     [(< ?lat-min ?latitude)]
     [(< ?latitude ?lat-max)]
     [(< ?lon-min ?longitude)]
     [(< ?longitude ?lat-max)]]

    [(airport-one-away-from ?source-code ?airport)
     [?route-1 :route/source-code ?source-code]
     [?route-1 :route/destination-id ?airport-id]
     [?airport :airport/id ?airport-id]]

    [(airport-two-away-from ?source-code ?airport)
     [?route-1 :route/source-code ?source-code]
     [?route-1 :route/destination-id ?stop-1-airport-id]
     [?route-2 :route/source-id ?stop-1-airport-id]
     [?route-2 :route/destination-id ?airport-id]
     [?airport :airport/id ?airport-id]]
    
    [(airport-not-in-schengen ?airport)
     [?country :country/schengen? false]
     [?country :country/name ?country-name]
     [?airport :airport/country ?country-name]]
    
    ])

(defn airports-two-away []
  (db/query
    '[:find ?city ?country
      :in $ %
      :where 
      (airport-two-away-from "TPE" ?airport)
      [?airport :airport/city ?city]
      [?airport :airport/country ?country]]
    rules))

(defn airports-within-bounds []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country
        :in $ % [?lon-min ?lat-min ?lon-max ?lat-max] 
        :where 
        (airport-within-bounds ?lon-min ?lat-min ?lon-max ?lat-max ?airport)
        [?airport :airport/city ?city]
        [?airport :airport/country ?country]]
      rules
      (parse-bounds bounds))))

(defn airports-one-away-within-bounds []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country
        :in $ % [?lon-min ?lat-min ?lon-max ?lat-max]
        :where 
        (airport-within-bounds ?lon-min ?lat-min ?lon-max ?lat-max ?airport)
        (airport-one-away-from "TPE")
        [?airport :airport/city ?city]
        [?airport :airport/country ?country]]
      rules
      (parse-bounds bounds))))

(defn airports-two-away-within-bounds []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query 
      '[:find ?city ?country
        :in $ % [?lon-min ?lat-min ?lon-max ?lat-max]
        :where
        (airport-within-bounds ?lon-min ?lat-min ?lon-max ?lat-max ?airport)
        (airport-two-away-from "TPE" ?airport)
        [?airport :airport/city ?city]
        [?airport :airport/country ?country]]
      rules
      (parse-bounds bounds))))

(defn airports-two-away-within-bounds-but-not-schengen []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country
        :in $ % [?lon-min ?lat-min ?lon-max ?lat-max]
        :where
        (airport-not-in-schengen ?airport)
        (airport-within-bounds ?lon-min ?lat-min ?lon-max ?lat-max ?airport)
        (airport-two-away-from "TPE" ?airport)
        [?airport :airport/city ?city]
        [?airport :airport/country ?country]]
      rules
      (parse-bounds bounds))))

(defn airports-within-bounds-but-not-schengen []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country
        :in $ % [?lon-min ?lat-min ?lon-max ?lat-max]
        :where
        (airport-not-in-schengen ?airport)
        (airport-within-bounds ?lon-min ?lat-min ?lon-max ?lat-max ?airport)
        [?airport :airport/country ?country]
        [?airport :airport/city ?city]]
      rules
      (parse-bounds bounds))))

