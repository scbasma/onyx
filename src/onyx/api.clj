(ns onyx.api
  (:require [clojure.core.async :refer [chan >!! <!! close! alts!! timeout go promise-chan]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info warn fatal error]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system :as system]
            [onyx.extensions :as extensions]
            [onyx.checkpoint :as checkpoint]
            [onyx.schema :as os]
            [schema.core :as s]
            [onyx.static.validation :as validator]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.uuid :refer [random-uuid]]
            [hasch.core :refer [edn-hash uuid5]])
  (:import [java.util UUID]
           [java.security MessageDigest]
           [onyx.system OnyxClient]))

(defn ^{:no-doc true} saturation [catalog]
  (let [rets
        (reduce #(+ %1 (or (:onyx/max-peers %2)
                           Double/POSITIVE_INFINITY))
                0
                catalog)]
    (if (zero? rets)
      Double/POSITIVE_INFINITY
      rets)))

(defn ^{:no-doc true} task-saturation [catalog tasks]
  (into
   {}
   (map
    (fn [task]
      {(:id task)
       (or (:onyx/max-peers (planning/find-task catalog (:name task)))
           Double/POSITIVE_INFINITY)})
    tasks)))

(defn ^{:no-doc true} min-required-peers [catalog tasks]
  (into
   {}
   (map
    (fn [task]
      {(:id task)
       (or (:onyx/min-peers (planning/find-task catalog (:name task))) 1)})
    tasks)))

(defn ^{:no-doc true} required-tags [catalog tasks]
  (reduce
   (fn [result [catalog-entry task]]
     (if-let [tags (:onyx/required-tags catalog-entry)]
       (assoc result (:id task) tags)
       result))
   {}
   (map vector catalog tasks)))

(defn ^{:no-doc true} flux-policies [catalog tasks]
  (->> tasks
       (map (fn [task]
              (vector (:id task)
                      (:onyx/flux-policy (planning/find-task catalog (:name task))))))
       (filter second)
       (into {})))

(defn ^{:added "0.6.0"} map-set-workflow->workflow
  "Converts a workflow in format:
   {:a #{:b :c}
    :b #{:d}}
   to format:
   [[:a :b]
    [:a :c]
    [:b :d]]"
  [workflow]
  (vec
   (reduce-kv (fn [w k v]
                (concat w
                        (map (fn [t] [k t]) v)))
              []
              workflow)))

(defn ^{:no-doc true} add-job-percentage [config job args]
  (if (= (:onyx.peer/job-scheduler config) :onyx.job-scheduler/percentage)
    (assoc args :percentage (:percentage job))
    args))

(defn ^{:no-doc true} task-id->pct [catalog task]
  (let [task-map (planning/find-task catalog (:name task))]
    {(:id task) (:onyx/percentage task-map)}))

(defn ^{:no-doc true} add-task-percentage [args job-id tasks catalog]
  (if (= (:task-scheduler args) :onyx.task-scheduler/percentage)
    (assoc-in args
              [:task-percentages]
              (into {} (map (fn [t] (task-id->pct catalog t)) tasks)))
    args))

(defn ^{:no-doc true} add-percentages-to-log-entry
  [config job args tasks catalog job-id]
  (let [job-updated (add-job-percentage config job args)]
    (add-task-percentage job-updated job-id tasks catalog)))

(defn ^{:no-doc true} find-input-tasks [catalog tasks]
  (mapv :id (filter (fn [task]
                      (let [task (planning/find-task catalog (:name task))]
                        (= :input (:onyx/type task))))
                    tasks)))

(defn ^{:no-doc true} find-output-tasks [catalog tasks]
  (mapv :id (filter (fn [task]
                     (let [task (planning/find-task catalog (:name task))]
                       (= :output (:onyx/type task))))
                   tasks)))

(defn ^{:no-doc true} find-grouped-tasks [catalog tasks]
  (mapv :id (filter (fn [task]
                      (let [task (planning/find-task catalog (:name task))]
                        (or (:onyx/group-by-key task) 
                            (:onyx/group-by-fn task))))
                    tasks)))

(defn ^{:no-doc true} find-state-tasks [windows]
  (vec (distinct (map :window/task windows))))

(defn ^{:no-doc true} expand-n-peers [catalog]
  (mapv
   (fn [entry]
     (if-let [n (:onyx/n-peers entry)]
       (assoc entry :onyx/min-peers n :onyx/max-peers n)
       entry))
   catalog))

(defn compact-graph [tasks]
  (->> tasks
       (map (juxt :id :egress-tasks))
       (into {})))

(defn ^{:no-doc true} create-submit-job-entry [id config job tasks]
  (let [task-ids (mapv :id tasks)
        job (update job :catalog expand-n-peers)
        scheduler (:task-scheduler job)
        sat (saturation (:catalog job))
        task-saturation (task-saturation (:catalog job) tasks)
        min-reqs (min-required-peers (:catalog job) tasks)
        task-flux-policies (flux-policies (:catalog job) tasks)
        input-task-ids (find-input-tasks (:catalog job) tasks)
        output-task-ids (find-output-tasks (:catalog job) tasks)
        group-task-ids (find-grouped-tasks (:catalog job) tasks)
        state-task-ids (find-state-tasks (:windows job))
        required-tags (required-tags (:catalog job) tasks)
        in->out (compact-graph tasks)
        args {:id id
              :tasks task-ids
              :task-scheduler scheduler
              :saturation sat
              :task-saturation task-saturation
              :min-required-peers min-reqs
              :flux-policies task-flux-policies
              :inputs input-task-ids
              :outputs output-task-ids
              :grouped group-task-ids
              :state state-task-ids
              :in->out in->out
              :required-tags required-tags}
        args (add-percentages-to-log-entry config job args tasks (:catalog job) id)]
    (create-log-entry :submit-job args)))

(defn validate-submission [job peer-client-config]
  (try
    (validator/validate-peer-client-config peer-client-config)
    (validator/validate-job job)
    {:success? true}
    (catch Throwable t
      (if-let [data (ex-data t)]
        (cond (and (:helpful-failed? data) (:e data))
              (throw (:e data))

              (:e data) {:success? false :e (:e data)}

              (:manual? data) {:success? false}

              :else (throw t))
        (throw t)))))

(defn ^{:no-doc true} hash-job [job]
  (let [x (str (uuid5 (edn-hash job)))
        md (MessageDigest/getInstance "SHA-256")]
    (.update md (.getBytes x "UTF-8"))
    (let [digest (.digest md)]
      (apply str (map #(format "%x" %) digest)))))

(defn ^{:no-doc true} serialize-job-to-zookeeper [client job-id job tasks entry]
  (extensions/write-chunk (:log client) :catalog (:catalog job) job-id)
  (extensions/write-chunk (:log client) :workflow (:workflow job) job-id)
  (extensions/write-chunk (:log client) :flow-conditions (:flow-conditions job) job-id)
  (extensions/write-chunk (:log client) :lifecycles (:lifecycles job) job-id)
  (extensions/write-chunk (:log client) :windows (:windows job) job-id)
  (extensions/write-chunk (:log client) :triggers (:triggers job) job-id)
  (extensions/write-chunk (:log client) :job-metadata (:metadata job) job-id)
  (doseq [task-resume-point (:resume-point job)]
    (extensions/write-chunk (:log client) :resume-point task-resume-point job-id))
  (doseq [task tasks]
    (extensions/write-chunk (:log client) :task task job-id))
  (extensions/write-log-entry (:log client) entry)
  {:success? true
   :job-id job-id})

(defmulti submit-job
  "Takes a connector and a job map,
   sending the job to the cluster for eventual execution. Returns a map
   with :success? indicating if the job was submitted to ZooKeeper. The job map
   may contain a :metadata key, among other keys described in the user
   guide. The :metadata key may optionally supply a :job-id value. Repeated
   submissions of a job with the same :job-id will be treated as an idempotent
   action. If a job has been submitted more than once, the original task IDs
   associated with the catalog will be returned, and the job will not run again,
   even if it has been killed or completed. If two or more jobs with the same
   :job-id are submitted, each will race to write a content-addressable hash
   value to ZooKeeper. All subsequent submitting jobs must match the hash value
   exactly, otherwise the submission will be rejected. This forces all jobs
   under the same :job-id to have exactly the same value.

   Takes either a peer configuration and constructs a client once for
   the operation (closing it on completion) or an already started client."
  ^{:added "0.6.0"}
  (fn [connector job]
    (type connector)))

(defmethod submit-job OnyxClient
  [{:keys [peer-config] :as onyx-client} job]
  (let [result (validate-submission job peer-config)]
    (if (:success? result)
      (let [job-hash (hash-job job)
            job (-> job
                    (update-in [:metadata :job-id] #(or % (random-uuid)))
                    (assoc-in [:metadata :job-hash] job-hash))
            id (get-in job [:metadata :job-id])
            tasks (planning/discover-tasks (:catalog job) (:workflow job))
            entry (create-submit-job-entry id peer-config job tasks)
            status (extensions/write-chunk (:log onyx-client) :job-hash job-hash id)]
        (if status
          (serialize-job-to-zookeeper onyx-client id job tasks entry)
          (let [written-hash (extensions/read-chunk (:log onyx-client) :job-hash id)]
            (if (= written-hash job-hash)
              (serialize-job-to-zookeeper onyx-client id job tasks entry)
              {:cause :incorrect-job-hash
               :success? false}))))
      result)))

(defmethod submit-job :default
  [peer-client-config job]
  (validator/validate-peer-client-config peer-client-config)
  (let [result (validate-submission job peer-client-config)]
    (if (:success? result)
      (let [onyx-client (component/start (system/onyx-client peer-client-config))]
        (try
          (submit-job onyx-client job)
          (finally
            (component/stop onyx-client))))
      result)))

(defmulti job-snapshot-coordinates
  "Reads the latest full snapshot coordinate stored for a given job-id and
   tenancy-id. This snapshot coordinate can be supplied to build-resume-point
   to build a full resume point.

  Takes either a peer configuration and constructs a client once for
   the operation (closing it on completion) or an already started client."
  ^{:added "0.10.0"}
  (fn [connector tenancy-id job-id]
    (type connector)))

(defmethod job-snapshot-coordinates OnyxClient
  [onyx-client tenancy-id job-id]
  (let [{:keys [log]} onyx-client]
    (info "Reading checkpoint coordinate at: " tenancy-id job-id)
    (try (checkpoint/read-checkpoint-coordinate log tenancy-id job-id)
         (catch org.apache.zookeeper.KeeperException$NoNodeException nne nil))))

(defmethod job-snapshot-coordinates :default
  [peer-client-config tenancy-id job-id]
  (validator/validate-peer-client-config peer-client-config)
  (s/validate os/TenancyId tenancy-id)
  (s/validate os/JobId job-id)
  (let [onyx-client (component/start (system/onyx-client peer-client-config))]
    (try
      (job-snapshot-coordinates onyx-client tenancy-id job-id)
      (finally
        (component/stop onyx-client)))))

(s/defn ^{:added "0.10.0"} build-resume-point :- os/ResumePoint
  "Builds a resume point for use in the :resume-point key
   of job data. This resume point will assume a direct mapping between
   the job resuming and the job it is resuming from. All tasks and windows should
   have the same name. Note, that it is safe to manipulate this data to allow resumption
   from jobs that are not identical, as long as you correctly map between task
   names, windows, etc."
  [{:keys [workflow catalog windows] :as new-job} :- os/Job coordinates :- os/ResumeCoordinate]
  (let [tasks (reduce into #{} workflow)
        task->task-map (into {} (map (juxt :onyx/name identity) catalog))
        task->windows (group-by :window/task windows)]
    (reduce (fn [m [task-id task-map]]
              (assoc m task-id 
                     (cond-> {}
                       (= :input (:onyx/type task-map)) 
                       (assoc :input (merge coordinates 
                                            {:mode :resume
                                             :task-id task-id 
                                             :slot-migration :direct}))

                       (= :output (:onyx/type task-map)) 
                       (assoc :output (merge coordinates 
                                             {:mode :resume
                                              :task-id task-id 
                                              :slot-migration :direct}))

                       (get task->windows task-id)
                       (assoc :windows (->> (get task->windows task-id)
                                            (map (fn [{:keys [window/id]}]
                                                   [id (merge coordinates {:mode :resume
                                                                           :task-id task-id
                                                                           :window-id id
                                                                           :slot-migration :direct})]))
                                            (into {}))))))
            {}
            task->task-map)))

(defmulti kill-job
  "Kills a currently executing job, given its job ID. All peers executing
   tasks for this job cleanly stop executing and volunteer to work on other jobs.
   Task lifecycle APIs for closing tasks are invoked. This job is never again scheduled
   for execution.

   Takes either a peer configuration and constructs a client once for
   the operation (closing it on completion) or an already started client."
  ^{:added "0.6.0"}
  (fn [connector job-id]
    (type connector)))

(defmethod kill-job OnyxClient
  [onyx-client job-id]
  (let [entry (create-log-entry :kill-job {:job (validator/coerce-uuid job-id)})]
    (extensions/write-log-entry (:log onyx-client) entry)
    true))

(defmethod kill-job :default
  [peer-client-config job-id]
  (validator/validate-peer-client-config peer-client-config)
  (when (nil? job-id)
    (throw (ex-info "Invalid job id" {:job-id job-id})))
  (let [client (component/start (system/onyx-client peer-client-config))]
    (try
      (kill-job client job-id)
      (finally
        (component/stop client)))))

(defmulti subscribe-to-log
  "Sends all events from the log to the provided core.async channel.
   Starts at the origin of the log and plays forward monotonically.

   Returns a map with keys :replica and :env. :replica contains the origin
   replica. :env contains an Component with a :log connection to ZooKeeper,
   convenient for directly querying the znodes. :env can be shutdown with
   the onyx.api/shutdown-env function.

   Takes either a peer configuration and constructs a client once for
   the operation or an already started client."
  ^{:added "0.6.0"}
  (fn [connector ch]
    (type connector)))

(defmethod subscribe-to-log OnyxClient
  [onyx-client ch]
  {:replica (extensions/subscribe-to-log (:log onyx-client) ch)
   :env onyx-client})

(defmethod subscribe-to-log :default
  [peer-client-config ch]
  (validator/validate-peer-client-config peer-client-config)
  (let [onyx-client (component/start (system/onyx-client peer-client-config))]
    (subscribe-to-log onyx-client ch)))

(defmulti gc
  "Invokes the garbage collector on Onyx. Compresses all local replicas
   for peers, decreasing memory usage. Also deletes old log entries from
   ZooKeeper, freeing up disk space.

   Local replicas clear out all data about completed and killed jobs -
   as if they never existed.

   Takes either a peer configuration and constructs a client once for
   the operation (closing it on completion) or an already started client."
  ^{:added "0.6.0"}
  (fn [connector]
    (type connector)))

(defmethod gc OnyxClient
  [onyx-client]
  (let [id (java.util.UUID/randomUUID)
        entry (create-log-entry :gc {:id id})
        ch (chan 1000)]
    (extensions/write-log-entry (:log onyx-client) entry)
    (loop [replica (extensions/subscribe-to-log (:log onyx-client) ch)]
      (let [entry (<!! ch)
            new-replica (extensions/apply-log-entry entry (assoc replica :version (:message-id entry)))]
        (if (and (= (:fn entry) :gc) (= (:id (:args entry)) id))
          (let [diff (extensions/replica-diff entry replica new-replica)
                args {:id id :type :client :log (:log onyx-client)}]
            (extensions/fire-side-effects! entry replica new-replica diff args))
          (recur new-replica))))
    true))

(defmethod gc :default
  [peer-client-config]
  (validator/validate-peer-client-config peer-client-config)
  (let [client (component/start (system/onyx-client peer-client-config))]
    (try
      (gc client)
      (finally
        (component/stop client)))))

(defmulti await-job-completion
  "Blocks until job-id has had all of its tasks completed or the job is killed.
   Returns true if the job completed successfully, false if the job was killed.

   Takes either a peer configuration and constructs a client once for
   the operation (closing it on completion) or an already started client."
  ^{:added "0.7.4"}
  (fn [connector & more]
    (type connector)))

(defmethod await-job-completion OnyxClient
  [onyx-client job-id timeout-ms]
  (let [job-id (validator/coerce-uuid job-id)
        ch (chan 100)
        tmt (if timeout-ms (timeout timeout-ms) (chan))]
    (loop [replica (extensions/subscribe-to-log (:log onyx-client) ch)]
      (let [[v c] (alts!! [(go (let [entry (<!! ch)]
                                 (extensions/apply-log-entry entry (assoc replica :version (:message-id entry)))))
                           tmt]
                          :priority true)]
        (cond (some #{job-id} (:completed-jobs v)) true
              (some #{job-id} (:killed-jobs v)) false
              (= c tmt) :timeout
              :else (recur v))))))

(defmethod await-job-completion :default
  ([peer-client-config job-id]
   (await-job-completion peer-client-config job-id nil))
  ([peer-client-config job-id timeout-ms]
   (validator/validate-peer-client-config peer-client-config)
   (let [onyx-client (component/start (system/onyx-client peer-client-config))]
     (try
       (await-job-completion onyx-client job-id timeout-ms)
       (finally
         (component/stop onyx-client))))))

(defn ^{:added "0.6.0"} start-peers
  "Launches n virtual peers. Each peer may be stopped by passing it to the shutdown-peer function."
  [n peer-group]
  (validator/validate-java-version)
  (mapv
   (fn [_]
     (let [group-ch (:group-ch (:peer-group-manager peer-group))
           peer-owner-id (random-uuid)]
       (>!! group-ch [:add-peer peer-owner-id])
       {:group-ch group-ch :peer-owner-id peer-owner-id}))
   (range n)))

(defn ^{:added "0.6.0"} shutdown-peer
  "Shuts down the virtual peer, which releases all of its resources
   and removes it from the execution of any tasks. This peer will
   no longer volunteer for tasks. Returns nil."
  [peer]
  (>!! (:group-ch peer) [:remove-peer (:peer-owner-id peer)]))

(defn ^{:added "0.8.1"} shutdown-peers
  "Like shutdown-peer, but takes a sequence of peers as an argument,
   shutting each down in order. Returns nil."
  [peers]
  (doseq [p peers]
    (shutdown-peer p)))

(defn ^{:added "0.6.0"} start-env
  "Starts a development environment using an in-memory implementation of ZooKeeper."
  [env-config]
  (validator/validate-env-config env-config)
  (component/start (system/onyx-development-env env-config)))

(defn ^{:added "0.6.0"} shutdown-env
  "Shuts down the given development environment."
  [env]
  (component/stop env))

(defn ^{:added "0.6.0"} start-peer-group
  "Starts a set of shared resources that are used across all virtual peers on this machine."
  [peer-config]
  (validator/validate-java-version)
  (validator/validate-peer-config peer-config)
  (component/start
    (system/onyx-peer-group peer-config)))

(defn ^{:added "0.6.0"} shutdown-peer-group
  "Shuts down the given peer-group"
  [peer-group]
  (component/stop peer-group))
