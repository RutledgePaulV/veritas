(ns io.github.rutledgepaulv.veritas.core
  "Bijections between structured data and datoms."
  (:require [clojure.walk :as walk]
            [datascript.core :as d])
  (:import (java.util List Map Set)))

(def ^:dynamic *state* nil)

(defn generate-temp-id []
  (swap! *state* dec))

(defprotocol IntoDatoms
  (into-datoms* [x]))

(defn into-datoms [x]
  (binding [*state* (or *state* (atom 0))]
    (into-datoms* x)))

(defn atomic? [x]
  (not (satisfies? IntoDatoms x)))

(extend-protocol IntoDatoms
  Set
  (into-datoms* [x]
    (let [map-id (generate-temp-id)]
      (into [[map-id :v/type :set]]
            (mapcat identity)
            (for [v x :let [element-id (generate-temp-id)]]
              (into [[map-id :v/elements element-id]]
                    (if (atomic? v)
                      [[element-id :v/value v]]
                      (let [reference-datoms (into-datoms v)]
                        (into reference-datoms
                              [[element-id :v/ref (ffirst reference-datoms)]]))))))))
  Map
  (into-datoms* [x]
    (let [map-id (generate-temp-id)]
      (into [[map-id :v/type :map]]
            (mapcat identity)
            (for [[k v] x
                  :let [element-id (generate-temp-id)]]
              (into [[map-id :v/elements element-id]]
                    (case [(atomic? k) (atomic? v)]
                      [true true]
                      [[element-id :v/key k]
                       [element-id :v/value v]]
                      [true false]
                      (let [value-datoms (into-datoms v)]
                        (into value-datoms
                              [[element-id :v/key k]
                               [element-id :v/value-ref (ffirst value-datoms)]]))
                      [false true]
                      (let [key-datoms (into-datoms k)]
                        (into key-datoms
                              [[element-id :v/key-ref (ffirst key-datoms)]
                               [element-id :v/value v]]))
                      [false false]
                      (let [key-datoms   (into-datoms k)
                            value-datoms (into-datoms v)]
                        (vec (concat key-datoms value-datoms
                                     [[element-id :v/key-ref (ffirst key-datoms)]
                                      [element-id :v/val-ref (ffirst value-datoms)]])))))))))
  List
  (into-datoms* [x]
    (let [list-id (generate-temp-id)]
      (into [[list-id :v/type :list]]
            (mapcat identity)
            (for [[index element] (map-indexed vector x)
                  :let [temp (generate-temp-id)]]
              (into [[list-id :v/elements temp]]
                    (if (atomic? element)
                      [[temp :v/value element]
                       [temp :v/index index]]
                      (let [reference-datoms (into-datoms element)]
                        (into reference-datoms
                              [[temp :v/ref (ffirst reference-datoms)]
                               [temp :v/index index]])))))))))


(defn dispatch [x] (:v/type x))

(defmulti realize #'dispatch)

(defmethod realize :default [x] x)

(defmethod realize :set [x]
  (set (map (fn [x]
              (if (contains? x :v/value)
                (:v/value x)
                (realize (:v/ref x))))
            (:v/elements x))))

(defmethod realize :list [x]
  (->> (:v/elements x [])
       (sort-by :v/index)
       (map (fn [x]
              (if (contains? x :v/value)
                (:v/value x)
                (realize (:v/ref x)))))
       (vec)))

(defmethod realize :map [x]
  (into {}
        (map (fn [x]
               [(if (contains? x :v/key)
                  (:v/key x)
                  (realize (:v/key-ref x)))
                (if (contains? x :v/value)
                  (:v/value x)
                  (realize (:v/value-ref x)))]))
        (:v/elements x)))

(defn pull-structure [db eid]
  (walk/postwalk realize (d/pull db '[*] eid)))

(def schemas
  {:v/ref
   {:db.type        :db.type/ref
    :db/isComponent true
    :db/valueType   :db.type/ref}
   :v/key-ref
   {:db.type        :db.type/ref
    :db/isComponent true
    :db/valueType   :db.type/ref}
   :v/value-ref
   {:db.type        :db.type/ref
    :db/isComponent true
    :db/valueType   :db.type/ref},
   :v/elements
   {:db.type        :db.type/ref,
    :db/isComponent true
    :db/valueType   :db.type/ref
    :db.cardinality :db.cardinality/many}})


(defn data->db [data]
  (-> (d/empty-db schemas)
      (d/db-with (for [d (into-datoms data)] (into [:db/add] d)))))

(defn round-trip [data]
  (pull-structure (data->db data) 1))


(comment

  (def data {:a :b :c [1 2 3]})
  (def db (data->db {:a :b :c [1 2 3]}))
  (def returned (pull-structure db 1))
  (= data returned)

  )
