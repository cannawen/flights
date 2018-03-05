(ns flights.queries
  (:require
    [flights.db :as db]))

(defn- parse-bounds [bounds-string]
  (-> bounds-string
      (clojure.string/split #",")
      (->> (map #(Float/parseFloat %)))))

(defn airports-two-away []
  (db/query
    '[:find ?city
      :where 
      [?route-1 :route/source-code "TPE"]
      [?route-1 :route/destination-id ?stop-1-airport-id]
      [?route-2 :route/source-id ?stop-1-airport-id]
      [?route-2 :route/destination-id ?airport-id]
      [?airport :airport/id ?airport-id]
      [?airport :airport/city ?city]]))

(defn airports-within-bounds []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country
        :in $ [?lon-min ?lat-min ?lon-max ?lat-max]
        :where 
        [?airport :airport/city ?city]
        [?airport :airport/country ?country]
        [?airport :airport/latitude ?latitude]
        [?airport :airport/longitude ?longitude]
        [(< ?lat-min ?latitude)]
        [(< ?latitude ?lat-max)]
        [(< ?lon-min ?longitude)]
        [(< ?longitude ?lat-max)]]
      (parse-bounds bounds))))

(defn airports-one-away-within-bounds []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country
        :in $ [?lon-min ?lat-min ?lon-max ?lat-max]
        :where 
        [?route :route/source-code "TPE"]
        [?route :route/destination-id ?airport-id]
        [?airport :airport/id ?airport-id]
        [?airport :airport/latitude ?latitude]
        [?airport :airport/longitude ?longitude]
        [(< ?lat-min ?latitude)]
        [(< ?latitude ?lat-max)]
        [(< ?lon-min ?longitude)]
        [(< ?longitude ?lat-max)]
        [?airport :airport/city ?city]
        [?airport :airport/country ?country]]
      (parse-bounds bounds))))

(defn airports-two-away-within-bounds []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query 
      '[:find ?city ?country
        :in $ [?lon-min ?lat-min ?lon-max ?lat-max]
        :where
        [?route-1 :route/source-code "TPE"]
        [?route-1 :route/destination-id ?stop-1-airport-id]
        [?route-2 :route/source-id ?stop-1-airport-id]
        [?route-2 :route/destination-id ?airport-id]
        [?airport :airport/id ?airport-id]
        [?airport :airport/latitude ?latitude]
        [?airport :airport/longitude ?longitude]
        [(< ?lat-min ?latitude)]
        [(< ?latitude ?lat-max)]
        [(< ?lon-min ?longitude)]
        [(< ?longitude ?lat-max)]
        [?airport :airport/country ?country]
        [?airport :airport/city ?city]]
      (parse-bounds bounds))))


(defn airports-two-away-within-bounds-but-not-schengen []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country-name
        :in $ [?lon-min ?lat-min ?lon-max ?lat-max]
        :where
        [?country :country/schengen? false]
        [?country :country/name ?country-name]
        [?airport :airport/country ?country-name]
        [?airport :airport/latitude ?latitude]
        [?airport :airport/longitude ?longitude]
        [(< ?lat-min ?latitude)]
        [(< ?latitude ?lat-max)]
        [(< ?lon-min ?longitude)]
        [(< ?longitude ?lat-max)]
        [?route-1 :route/source-code "TPE"]
        [?route-1 :route/destination-id ?stop-1-airport-id]
        [?route-2 :route/source-id ?stop-1-airport-id]
        [?route-2 :route/destination-id ?airport-id]
        [?airport :airport/id ?airport-id]
        [?airport :airport/city ?city]]
      (parse-bounds bounds))))

(defn airports-within-bounds-but-not-schengen []
  (let [bounds "7.1,32.4,43.5,48.46"]
    (db/query
      '[:find ?city ?country-name
        :in $ [?lon-min ?lat-min ?lon-max ?lat-max]
        :where
        [?country :country/schengen? false]
        [?country :country/name ?country-name]
        [?airport :airport/country ?country-name]
        [?airport :airport/city ?city]
        [?airport :airport/latitude ?latitude]
        [?airport :airport/longitude ?longitude]
        [(< ?lat-min ?latitude)]
        [(< ?latitude ?lat-max)]
        [(< ?lon-min ?longitude)]
        [(< ?longitude ?lat-max)]]
      (parse-bounds bounds))))

