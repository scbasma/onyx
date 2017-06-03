(ns onyx.peer.acker-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.plugin.core-async]
            [onyx.api]))

(def n-messages 1000)

(def in-chan (atom nil))

(def out-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def in-calls {:lifecycle/before-task-start inject-in-ch})

(def out-calls {:lifecycle/before-task-start inject-out-ch})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn my-identity [segment]
  segment)

(deftest test-multiple-ackers
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        _ (println "Inside test multiple ackers")]
    (with-test-env [test-env [4 env-config peer-config]]
      (let [batch-size 40
            catalog [{:onyx/name :in
                      :onyx/plugin :onyx.plugin.core-async/input
                      :onyx/type :input
                      :onyx/medium :core.async
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Reads segments from a core.async channel"}

                     {:onyx/name :inc
                      :onyx/fn :onyx.peer.acker-test/my-inc
                      :onyx/type :function
                      :onyx/batch-size batch-size}

                     {:onyx/name :identity
                      :onyx/fn :onyx.peer.acker-test/my-identity
                      :onyx/type :function
                      :onyx/batch-size batch-size}

                     {:onyx/name :out
                      :onyx/plugin :onyx.plugin.core-async/output
                      :onyx/type :output
                      :onyx/medium :core.async
                      :onyx/batch-size batch-size
                      :onyx/max-peers 1
                      :onyx/doc "Writes segments to a core.async channel"}]
            workflow [[:in :inc]
                      [:inc :identity]
                      [:identity :out]]
            lifecycles [{:lifecycle/task :in
                         :lifecycle/calls ::in-calls}
                        {:lifecycle/task :in
                         :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                        {:lifecycle/task :out
                         :lifecycle/calls ::out-calls}
                        {:lifecycle/task :out
                         :lifecycle/calls :onyx.plugin.core-async/writer-calls}]]

        (reset! in-chan (chan (inc n-messages)))
        (reset! out-chan (chan (sliding-buffer (inc n-messages))))

        (doseq [n (range n-messages)]
          (>!! @in-chan {:n n}))
        (>!! @in-chan :done)
        (println "before submit-job in acker-test")
        (onyx.api/submit-job
          peer-config
          {:catalog catalog
           :workflow workflow
           :lifecycles lifecycles
           :task-scheduler :onyx.task-scheduler/balanced
           :acker/percentage 5
           :acker/exempt-input-tasks? true
           :acker/exempt-output-tasks? true
           :acker/exempt-tasks [:inc]})
        (println "after submit-job!")

        (let [_ (println "before results and expected!!")
              results (doall (repeatedly (inc n-messages) (fn [] (<!! @out-chan))))
              _ (println "after results!")
              expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))
              _ (println "inside results and expected!!")]
          (is (= expected (set (butlast results))))
          (is (= :done (last results))))))))
