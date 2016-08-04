(ns puppetlabs.trapperkeeper.services.watcher.filesystem-watch-core
  (:require [clojure.tools.logging :as log]
            [me.raynes.fs :as fs]
            [schema.core :as schema]
            [puppetlabs.i18n.core :refer [trs]]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.trapperkeeper.services.protocols.filesystem-watch-service :refer [Event Watcher]])
  (:import (clojure.lang Atom IFn)
           (java.nio.file StandardWatchEventKinds Path WatchEvent WatchKey FileSystems ClosedWatchServiceException)
           (com.puppetlabs DirWatchUtils)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def event-type-mappings
  {StandardWatchEventKinds/ENTRY_CREATE :create
   StandardWatchEventKinds/ENTRY_MODIFY :modify
   StandardWatchEventKinds/ENTRY_DELETE :delete
   StandardWatchEventKinds/OVERFLOW :unknown})

(def window-min 100)

(def window-max 2000)

(def window-units java.util.concurrent.TimeUnit/MILLISECONDS)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Functions
;;;
;;; Helper functions in this namespace are heavily influenced by WatchDir.java
;;; from Java's official documentation:
;;; https://docs.oracle.com/javase/tutorial/essential/io/notification.html
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(schema/defn clojurize :- Event
  [event :- WatchEvent
   watch-path :- Path]
  {:type (get event-type-mappings (.kind event))
   :count (.count event)
   :path (when-not (= StandardWatchEventKinds/OVERFLOW (.kind event))
           (fs/file watch-path (.context event)))
   :parent watch-path
   :item (when-not (= StandardWatchEventKinds/OVERFLOW (.kind event))
           (.context event))})

(defn pprint-events
  [events]
  (->> events
       (map #(update % :path str))
       ks/pprint-to-string))

(defn validate-watch-options!
  [options]
  (when-not (= true (:recursive options))
    (throw
      (IllegalArgumentException.
        (trs "Support for non-recursive directory watching not yet implemented")))))

(defrecord WatcherImpl
  [watch-service callbacks]
  Watcher
  (add-watch-dir!
    [this dir options]
    (validate-watch-options! options)
    (DirWatchUtils/registerRecursive watch-service
                                     [(.toPath (fs/file dir))]))
  (add-callback!
    [this callback]
    (swap! callbacks conj callback)))

(defn create-watcher
  []
  (map->WatcherImpl
    {:watch-service (.newWatchService (FileSystems/getDefault))
     :callbacks (atom [])}))

(schema/defn watch-new-directories!
  [events :- [Event]
   watcher :- (schema/protocol Watcher)]
  (let [dir-create? (fn [event]
                      (and (= :create (:type event))
                           (fs/directory? (:path event))))]
    (DirWatchUtils/registerRecursive (:watch-service watcher)
                                     (->> events
                                          (filter dir-create?)
                                          (map #(.toPath (:path %)))))))

(defn get-events
  [watch-key]
  (map #(clojurize % (.watchable watch-key)) (vec (.pollEvents watch-key))))

(schema/defn retrieve-events
  :- [Event]
  "Blocks until an event the watcher is concerned with has occured.
  Returns the native WatchKey and WatchEvents"
  [watcher :- (schema/protocol Watcher)]
  (let [watch-key (.take (:watch-service watcher))
        events (get-events watch-key)
        time-limit (+ (System/currentTimeMillis) window-max)]
    (watch-new-directories! events watcher)
    (.reset watch-key)
    (let [final-events (loop [e events]
                         (if-let [waiting-key (.poll (:watch-service watcher) window-min window-units)]
                           (let [waiting-events (get-events waiting-key)]
                             (watch-new-directories! waiting-events watcher)
                             (.reset waiting-key)
                             (if (< (System/currentTimeMillis) time-limit)
                               (recur (into e waiting-events))
                               (into e waiting-events)))
                             e))]
      final-events)))

(schema/defn process-events!
  "Process for side-effects any events that occured for watcher's watch-key"
  [watcher :- (schema/protocol Watcher)
   events :- [Event]
   shutdown-fn :- IFn]
  (let [callbacks @(:callbacks watcher)]
    (log/info (trs "Got {0} event(s) across path(s) {1}"
                   (count events) (map #(:path %) events)))
    (log/debugf "%s\n%s"
                (trs "Events:")
                (pprint-events events))
    (shutdown-fn #(doseq [callback callbacks]
                   (callback events)))))

(schema/defn watch!
  "Creates a future and processes events for the passed in watcher.
  The future will continue until the underlying WatchService is closed."
  [watcher :- (schema/protocol Watcher)
   shutdown-fn :- IFn]
  (future
    (let [stopped? (atom false)]
      (while (not @stopped?)
        (try
          (let [events (retrieve-events watcher)]
            (when-not (empty? events)
              (process-events! watcher events shutdown-fn)))
         (catch ClosedWatchServiceException e
           (reset! stopped? true)
           (log/info (trs "Closing watcher {0}" watcher)))
         (catch Exception e
           (log/error e)))))))

