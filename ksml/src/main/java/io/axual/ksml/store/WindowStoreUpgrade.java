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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Clears window store state that KSML 1.x wrote in a format this version can't read, once per application state
 * directory, so Kafka Streams restores it from the changelog (design D7).
 * <p>
 * KSML 1.x built a persistent window store with {@code timestamped: true} on a timestamped RocksDB store, but wrote
 * plain values into it. This version reads that store as timestamped, and can't tell the old values apart. Their
 * changelog holds the values in a form that the restore converts correctly.
 */
@Slf4j
public final class WindowStoreUpgrade {
    /** Marks an application state directory as checked. */
    public static final String MARKER_FILE = ".ksml-store-format";
    private static final String CHECKPOINT_FILE = ".checkpoint";
    private static final String POSITION_FILE_SUFFIX = ".position";
    // Kafka Streams names task directories <subtopology>_<partition>
    private static final Pattern TASK_DIRECTORY = Pattern.compile("\\d+_\\d+");

    private WindowStoreUpgrade() {
    }

    /**
     * Deletes the local state of persistent, timestamped window stores in every task directory, unless the application
     * state directory is marked already. Then it marks the directory.
     *
     * @param applicationDirectory the state directory of the application: {@code <state.dir>/<application.id>}
     * @param stores               the stores built for Python functions
     */
    public static void prepare(Path applicationDirectory, Collection<? extends StateStoreDefinition> stores) {
        final var marker = applicationDirectory.resolve(MARKER_FILE);
        if (Files.exists(marker)) return;

        final var windowStores = stores.stream()
                .filter(WindowStateStoreDefinition.class::isInstance)
                .map(WindowStateStoreDefinition.class::cast)
                .filter(store -> store.persistent() && store.timestamped())
                .toList();
        try {
            if (Files.isDirectory(applicationDirectory) && !windowStores.isEmpty()) {
                try (var paths = Files.list(applicationDirectory)) {
                    for (final var task : paths.filter(WindowStoreUpgrade::isTaskDirectory).toList()) {
                        cleanTask(task, windowStores);
                    }
                }
            }
            Files.createDirectories(applicationDirectory);
            Files.writeString(marker, "Window store state checked by KSML. Kafka Streams state in this directory is in the KSML 2 format.\n");
        } catch (IOException e) {
            throw new ExecutionException("Could not check the window store state in " + applicationDirectory, e);
        }
    }

    private static boolean isTaskDirectory(Path path) {
        return Files.isDirectory(path) && TASK_DIRECTORY.matcher(path.getFileName().toString()).matches();
    }

    private static void cleanTask(Path task, List<WindowStateStoreDefinition> windowStores) throws IOException {
        boolean deleted = false;
        for (final var store : windowStores) {
            final var storeDirectory = task.resolve(store.name());
            final var positionFile = task.resolve(store.name() + POSITION_FILE_SUFFIX);
            if (!Files.exists(storeDirectory) && !Files.exists(positionFile)) continue;

            deleteRecursively(storeDirectory);
            Files.deleteIfExists(positionFile);
            deleted = true;
            if (store.logging()) {
                log.info("Deleted the local state of window store {} in task {}, written by KSML 1.x: Kafka Streams restores it from the changelog",
                        store.name(), task.getFileName());
            } else {
                log.warn("Deleted the local state of window store {} in task {}, written by KSML 1.x: the store has no changelog, so its data is lost",
                        store.name(), task.getFileName());
            }
        }
        // Kafka Streams would load the offsets of the deleted stores from the checkpoint and skip their restore
        if (deleted) Files.deleteIfExists(task.resolve(CHECKPOINT_FILE));
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
