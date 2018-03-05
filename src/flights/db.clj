(ns flights.db
  (:require
    [flights.db-datomic :as datomic]
    [flights.db-datascript :as datascript]))

(defonce db (atom nil))

(defn use! [choice]
  (case choice
    :datomic (reset! db :datomic)
    :datascript (reset! db :datascript)
    (println "Only :datomic or :datascript supported.")))

(defn init! []
  (case @db
    :datomic (datomic/init!)
    :datascript (datascript/init!)
    (println "You must first pick a database using (use! ...)")))

(defn transact! [& args]
  (case @db
    :datomic (apply datomic/transact! args)
    :datascript (apply datascript/transact! args)
    (println "You must first pick a database using (use! ...)")))

(defn query [& args]
  (case @db 
    :datomic (apply datomic/query args)
    :datascript (apply datascript/query args)
    (println "You must first pick a database using (use! ...)")))
