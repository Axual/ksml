package io.axual.ksml.store;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.ExecutionException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Checks the local state that KSML 1.x left, once per application state directory, before Kafka Streams starts.
 * Stops when state would be lost, and clears 1.x window store state that has a changelog.
 * See "Declared stores use the Kafka Streams defaults" in the migration guide.
 */
@Slf4j
public final class StoreUpgrade {
    /** Marks an application state directory as checked. */
    public static final String MARKER_FILE = ".ksml-store-format";
    private static final String CHECKPOINT_FILE = ".checkpoint";
    private static final String POSITION_FILE_SUFFIX = ".position";
    private static final String SST_FILE_SUFFIX = ".sst";
    private static final String BLOB_FILE_SUFFIX = ".blob";
    // Kafka Streams keeps key-value and versioned stores in this subdirectory of the task, other stores in the task
    private static final String KEY_VALUE_STORE_DIRECTORY = "rocksdb";
    // Kafka Streams names task directories <subtopology>_<partition>
    private static final Pattern TASK_DIRECTORY = Pattern.compile("\\d+_\\d+");

    private StoreUpgrade() {
    }

    /**
     * The shared store of a stream-stream left or outer join. Kafka Streams creates it, names it after
     * {@code thisStore}, and makes it a RocksDB store when {@code thisStore} is persistent.
     *
     * @param name       the name that Kafka Streams gives the store
     * @param thisStore  the declared {@code thisStore} of the join
     * @param otherStore the declared {@code otherStore} of the join
     */
    public record SharedJoinStore(String name, StateStoreDefinition thisStore, StateStoreDefinition otherStore) {
        boolean persistent() {
            return thisStore.persistent();
        }

        // As BaseOperation.streamJoinedOf decides it
        boolean logging() {
            return thisStore.logging() || otherStore.logging();
        }

        // KSML 1.x logged the shared store only when thisStore set logging: true
        boolean loggedInKsml1() {
            return Boolean.TRUE.equals(thisStore.declaredLogging());
        }
    }

    /**
     * Checks the KSML 1.x local state of the stores in every task directory, unless the application state directory
     * is marked already. Throws when state would be lost, and changes nothing then. Otherwise it deletes the 1.x
     * state of timestamped window stores for Python that have a changelog, and marks the directory. Without
     * persistent stores it does nothing.
     *
     * @param applicationDirectory the state directory of the application: {@code <state.dir>/<application.id>}
     * @param stores               the stores to check, see {@code TopologyGenerator.upgradeCheckedStores()}
     * @param pythonStores         the stores among them that are built for Python functions
     * @param sharedJoinStores     the shared stores of stream-stream left and outer joins
     * @param wipesTasks           whether Kafka Streams deletes a whole task when a logged store in it has local
     *                             state but no changelog offset: with exactly-once processing, without transactional
     *                             state stores
     */
    public static void prepare(Path applicationDirectory,
                               Collection<? extends StateStoreDefinition> stores,
                               Collection<? extends StateStoreDefinition> pythonStores,
                               Collection<SharedJoinStore> sharedJoinStores,
                               boolean wipesTasks) {
        final var marker = applicationDirectory.resolve(MARKER_FILE);
        if (Files.exists(marker)) return;
        // No 1.x state can exist, so leave the state directory alone, as Kafka Streams does
        if (stores.stream().noneMatch(StateStoreDefinition::persistent)
                && sharedJoinStores.stream().noneMatch(SharedJoinStore::persistent)) return;

        final var unreadableStores = pythonStores.stream()
                .filter(WindowStateStoreDefinition.class::isInstance)
                .filter(store -> store.persistent() && Boolean.TRUE.equals(store.declaredTimestamped()))
                .toList();
        final var unreadableNames = unreadableStores.stream().map(StateStoreDefinition::name).collect(Collectors.toSet());
        final var unloggedStores = stores.stream()
                .filter(store -> store.persistent() && store.declaredLogging() == null)
                .filter(store -> !unreadableNames.contains(store.name()))
                .toList();
        final var unloggedSharedJoinStores = sharedJoinStores.stream()
                .filter(store -> store.persistent() && store.logging() && !store.loggedInKsml1())
                .toList();

        try {
            final var tasks = taskDirectories(applicationDirectory);
            final var restorable = restorableTasks(unreadableStores, tasks);
            final var problems = new ArrayList<String>();
            problems.addAll(unreadableProblems(unreadableStores, tasks));
            problems.addAll(unloggedProblems(unloggedStores, tasks));
            problems.addAll(unloggedSharedJoinProblems(unloggedSharedJoinStores, tasks));
            if (wipesTasks) problems.addAll(wipedTaskProblems(restorable, stores, sharedJoinStores));
            if (!problems.isEmpty()) {
                throw new ExecutionException("KSML 1.x left local state in " + applicationDirectory
                        + " that this version would lose. Nothing was changed. Fix these stores and start again, see the guide 'Upgrading to 2.0.0':\n"
                        + String.join("\n", problems));
            }

            for (final var entry : restorable.entrySet()) {
                deleteWindowState(entry.getKey(), entry.getValue());
            }
            Files.createDirectories(applicationDirectory);
            Files.writeString(marker, "KSML 1.x store state checked.\n");
        } catch (IOException e) {
            throw new ExecutionException("Could not check the KSML 1.x store state in " + applicationDirectory, e);
        }
    }

    private static List<Path> taskDirectories(Path applicationDirectory) throws IOException {
        if (!Files.isDirectory(applicationDirectory)) return List.of();
        try (var paths = Files.list(applicationDirectory)) {
            return paths.filter(StoreUpgrade::isTaskDirectory).sorted().toList();
        }
    }

    private static boolean isTaskDirectory(Path path) {
        return Files.isDirectory(path) && TASK_DIRECTORY.matcher(path.getFileName().toString()).matches();
    }

    // Any files of the store, which the delete before the restore clears
    private static boolean hasWindowState(Path task, String store) {
        return Files.exists(task.resolve(store)) || Files.exists(task.resolve(store + POSITION_FILE_SUFFIX));
    }

    // Data that would be lost. RocksDB creates its other files when it opens a store, also an empty one
    private static boolean hasLocalState(Path task, String store) {
        return hasDataFiles(task.resolve(store)) || hasDataFiles(task.resolve(KEY_VALUE_STORE_DIRECTORY).resolve(store));
    }

    // Kafka Streams turns off the RocksDB write-ahead log, so saved data is always in .sst files, or in .blob files
    // when a rocksdb.config.setter turns on BlobDB
    private static boolean hasDataFiles(Path directory) {
        if (!Files.isDirectory(directory)) return false;
        try (var files = Files.walk(directory)) {
            return files.map(file -> file.getFileName().toString())
                    .anyMatch(name -> name.endsWith(SST_FILE_SUFFIX) || name.endsWith(BLOB_FILE_SUFFIX));
        } catch (IOException _) {
            // If unsure, treat it as state and stop
            return true;
        }
    }

    /** The tasks with 1.x state of unreadable window stores that have a changelog, with those stores. */
    private static Map<Path, List<StateStoreDefinition>> restorableTasks(List<? extends StateStoreDefinition> unreadableStores, List<Path> tasks) {
        final var restorable = new LinkedHashMap<Path, List<StateStoreDefinition>>();
        for (final var store : unreadableStores) {
            if (!Boolean.TRUE.equals(store.declaredLogging())) continue;
            tasks.stream()
                    .filter(task -> hasWindowState(task, store.name()))
                    .forEach(task -> restorable.computeIfAbsent(task, _ -> new ArrayList<>()).add(store));
        }
        return restorable;
    }

    private static List<String> unreadableProblems(List<? extends StateStoreDefinition> unreadableStores, List<Path> tasks) {
        final var problems = new ArrayList<String>();
        for (final var store : unreadableStores) {
            if (Boolean.TRUE.equals(store.declaredLogging())) continue;
            final var storeTasks = tasks.stream().filter(task -> hasLocalState(task, store.name())).toList();
            if (!storeTasks.isEmpty()) problems.add(unreadableProblem(store, storeTasks));
        }
        return problems;
    }

    private static List<String> unloggedProblems(List<? extends StateStoreDefinition> unloggedStores, List<Path> tasks) {
        final var problems = new ArrayList<String>();
        for (final var store : unloggedStores) {
            final var storeTasks = tasks.stream().filter(task -> hasLocalState(task, store.name())).toList();
            if (!storeTasks.isEmpty()) problems.add(unloggedProblem(store, storeTasks));
        }
        return problems;
    }

    private static List<String> unloggedSharedJoinProblems(List<SharedJoinStore> sharedJoinStores, List<Path> tasks) {
        final var problems = new ArrayList<String>();
        for (final var store : sharedJoinStores) {
            final var storeTasks = tasks.stream().filter(task -> hasLocalState(task, store.name())).toList();
            if (!storeTasks.isEmpty()) problems.add(unloggedSharedJoinProblem(store, storeTasks));
        }
        return problems;
    }

    private static List<String> wipedTaskProblems(Map<Path, List<StateStoreDefinition>> restorable,
                                                  Collection<? extends StateStoreDefinition> stores,
                                                  Collection<SharedJoinStore> sharedJoinStores) {
        final var problems = new ArrayList<String>();
        for (final var entry : restorable.entrySet()) {
            final var task = entry.getKey();
            final var keptStores = Stream.concat(
                            stores.stream()
                                    .filter(store -> store.persistent() && !store.logging())
                                    .map(StateStoreDefinition::name),
                            sharedJoinStores.stream()
                                    .filter(store -> store.persistent() && !store.logging())
                                    .map(SharedJoinStore::name))
                    .filter(name -> hasLocalState(task, name))
                    .toList();
            if (!keptStores.isEmpty()) problems.add(wipedTaskProblem(task, entry.getValue(), keptStores));
        }
        return problems;
    }

    private static String unloggedProblem(StateStoreDefinition store, List<Path> tasks) {
        return "- Store '" + store.name() + "'" + hasLocalStateIn(tasks)
                + " and leaves out 'logging'. In KSML 1.x it had no changelog, now it gets a new, empty one."
                + " Set 'logging: false' to keep the KSML 1.x behavior, or 'logging: true' to accept a changelog without the old data.";
    }

    private static String unreadableProblem(StateStoreDefinition store, List<Path> tasks) {
        final var paths = tasks.stream()
                .flatMap(task -> Stream.of(task.resolve(store.name()), task.resolve(store.name() + POSITION_FILE_SUFFIX)))
                .map(Path::toString)
                .collect(Collectors.joining(", "));
        return "- Window store '" + store.name() + "'" + hasLocalStateIn(tasks)
                + " that KSML 1.x wrote in a format this version can't read, and no changelog to restore it from."
                + " Delete " + paths + " (the data is lost), give the store a new name, or set 'logging: true' on KSML 1.x first.";
    }

    private static String unloggedSharedJoinProblem(SharedJoinStore store, List<Path> tasks) {
        final var thisStore = store.thisStore().name();
        final var otherStore = store.otherStore().name();
        return "- The shared store '" + store.name() + "' of the join of '" + thisStore + "' and '" + otherStore
                + "'" + hasLocalStateIn(tasks)
                + ". In KSML 1.x it had no changelog, now it gets a new, empty one."
                + " Set 'logging: false' on both '" + thisStore + "' and '" + otherStore + "' to keep the KSML 1.x behavior,"
                + " or 'logging: true' on '" + thisStore + "' to accept a changelog without the old data.";
    }

    private static String wipedTaskProblem(Path task, List<? extends StateStoreDefinition> deletedStores, List<String> keptStores) {
        return "- Task " + task.getFileName() + ": KSML clears window store "
                + quoted(deletedStores.stream().map(StateStoreDefinition::name).toList())
                + ", and with exactly-once processing Kafka Streams then deletes the whole task, also the state of "
                + quoted(keptStores) + " with 'logging: false'."
                + " Start once with 'processing.guarantee: at_least_once', then switch back.";
    }

    private static String quoted(List<String> names) {
        return names.stream().map(name -> "'" + name + "'").collect(Collectors.joining(", "));
    }

    private static String hasLocalStateIn(List<Path> tasks) {
        return " has local state in task " + taskNames(tasks);
    }

    private static String taskNames(List<Path> tasks) {
        return tasks.stream().map(task -> task.getFileName().toString()).collect(Collectors.joining(", "));
    }

    private static void deleteWindowState(Path task, List<StateStoreDefinition> stores) throws IOException {
        // Kafka Streams would load the offsets of the deleted stores from the checkpoint and skip their restore.
        // Deleted first, so a crash halfway leaves the stores, which the next start finds and deletes again.
        Files.deleteIfExists(task.resolve(CHECKPOINT_FILE));
        for (final var store : stores) {
            deleteRecursively(task.resolve(store.name()));
            Files.deleteIfExists(task.resolve(store.name() + POSITION_FILE_SUFFIX));
            log.info("Deleted the KSML 1.x local state of window store '{}' in task '{}': Kafka Streams restores it from the changelog",
                    store.name(), task.getFileName());
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) return;
        try (var paths = Files.walk(path)) {
            for (final var file : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(file);
            }
        }
    }
}
