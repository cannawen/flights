(ns flights.core
  (:require
    [clojure.data.csv :as csv]
    [datascript.core :as d]))

(defn routes []
  (->> "data/routes.csv"
       slurp
       csv/read-csv
       (remove (fn [row]
                  (some (fn [item] (= item "\\N")) row)))
       (map (fn [row]
              {:source-id (Integer/parseInt (row 3))
               :source-code (row 2)
               :destination-code (row 4)
               :destination-id (Integer/parseInt (row 5))}))))

(defn airports []
  (->> "data/airports.csv"
       slurp
       csv/read-csv
       (map (fn [row]
              {:latitude (Float/parseFloat (row 6))
               :longitude (Float/parseFloat (row 7))
               :city (row 2)
               :country (row 3)
               :id (Integer/parseInt (row 0))}))))  

(defonce conn (d/create-conn {}))   
  
(defn import! []
  (println "Adding airports...")                       
  (d/transact! conn (airports))
  (println "Adding routes...")
  (d/transact! conn (routes))
  nil)
  
(defn airports-two-away []
  (d/q '[:find  ?city
         :where [?airport :city ?city]
                [?airport :id ?airport-id]
                [?route-2 :destination-id ?airport-id]
                [?route-2 :source-id ?stop-1-airport-id]
                [?route-1 :destination-id ?stop-1-airport-id]
                [?route-1 :source-code "TPE"]]
    @conn))


(defn airports-within-bounds []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find  ?city ?country
           :in $ ?lon-min ?lat-min ?lon-max ?lat-max
           :where [?airport :city ?city]
                  [?airport :country ?country]
                  [?airport :latitude ?latitude]
                  [?airport :longitude ?longitude]
                  [(< ?lat-min ?latitude ?lat-max)]
                  [(< ?lon-min ?longitude ?lon-max)]]
      @conn
      lon-min lat-min lon-max lat-max)))

(defn airports-one-away-within-bounds []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find  ?city ?country
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
      @conn
      lon-min lat-min lon-max lat-max)))

(defn airports-two-away-within-bounds []
  (let [box "7.1,32.4,43.5,48.46"
        bounds (-> box
                   (clojure.string/split #",")
                   (->> (map #(Float/parseFloat %))))
        [lon-min lat-min lon-max lat-max] bounds]
    (d/q '[:find  ?city ?country
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
      @conn
      lon-min lat-min lon-max lat-max)))

(def run airports-two-away-within-bounds)
