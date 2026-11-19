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


import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.store.StoreUpgrade.SharedJoinStore;
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;

import static io.axual.ksml.testutil.LogCapture.stderrOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The one-time check of the local state that KSML 1.x left: it stops when state would be lost, and clears window
 * store state that this version can't read but can restore from the changelog.
 */
class StoreUpgradeTest {
    private static final UserType UNKNOWN = UserType.UNKNOWN;
    private static final String SHARED_JOIN_STORE = "this_store-outer-shared-join-store";

    @TempDir
    Path stateDirectory;

    private static WindowStateStoreDefinition windowStore(String name, boolean persistent, Boolean timestamped, Boolean logging) {
        return new WindowStateStoreDefinition(name, persistent, timestamped, Duration.ofHours(1), Duration.ofMinutes(1), false, UNKNOWN, UNKNOWN, false, logging);
    }

    private static KeyValueStateStoreDefinition keyValueStore(String name, boolean persistent, Boolean logging) {
        return new KeyValueStateStoreDefinition(name, persistent, null, false, null, null, UNKNOWN, UNKNOWN, false, logging);
    }

    // The shared store of an outer join of the stores 'this_store' and 'other_store'
    private static SharedJoinStore sharedJoinStore(boolean persistent, Boolean thisLogging, Boolean otherLogging) {
        return new SharedJoinStore(SHARED_JOIN_STORE, windowStore("this_store", persistent, null, thisLogging), windowStore("other_store", persistent, null, otherLogging));
    }

    private Path applicationDirectory() {
        return stateDirectory.resolve("app");
    }

    private void prepare(List<? extends StateStoreDefinition> stores) {
        // As in a topology where Python functions use every store
        prepare(stores, List.of());
    }

    private void prepare(List<? extends StateStoreDefinition> stores, List<SharedJoinStore> sharedJoinStores) {
        StoreUpgrade.prepare(applicationDirectory(), stores, stores, sharedJoinStores, false);
    }

    private void prepareWithExactlyOnce(List<? extends StateStoreDefinition> stores) {
        prepareWithExactlyOnce(stores, List.of());
    }

    private void prepareWithExactlyOnce(List<? extends StateStoreDefinition> stores, List<SharedJoinStore> sharedJoinStores) {
        StoreUpgrade.prepare(applicationDirectory(), stores, stores, sharedJoinStores, true);
    }

    private Path createWindowStore(String task, String store) throws IOException {
        final var storeDirectory = applicationDirectory().resolve(task).resolve(store);
        Files.createDirectories(storeDirectory.resolve(store + ".1794319200000"));
        Files.writeString(storeDirectory.resolve(store + ".1794319200000").resolve("000010.sst"), "data");
        Files.writeString(applicationDirectory().resolve(task).resolve(store + ".position"), "position");
        return storeDirectory;
    }

    private Path createKeyValueStore(String task, String store) throws IOException {
        final var storeDirectory = applicationDirectory().resolve(task).resolve("rocksdb").resolve(store);
        Files.createDirectories(storeDirectory);
        Files.writeString(storeDirectory.resolve("000010.sst"), "data");
        return storeDirectory;
    }

    // The files that RocksDB creates when it opens a store, without data, as KSML 1.3.x leaves them after a clean stop
    private static Path createEmptyRocksDb(Path directory) throws IOException {
        Files.createDirectories(directory);
        for (final var file : List.of("000004.log", "CURRENT", "IDENTITY", "LOCK", "LOG", "MANIFEST-000005", "OPTIONS-000007")) {
            Files.writeString(directory.resolve(file), "");
        }
        return directory;
    }

    private Path createCheckpoint(String task) throws IOException {
        Files.createDirectories(applicationDirectory().resolve(task));
        return Files.writeString(applicationDirectory().resolve(task).resolve(".checkpoint"), "checkpoint");
    }

    private Path marker() {
        return applicationDirectory().resolve(StoreUpgrade.MARKER_FILE);
    }

    @Test
    @DisplayName("deletes the timestamped window stores with a changelog of every task, with their position files and the checkpoint of their task")
    void deletesRestorableWindowStoreState() throws IOException {
        createWindowStore("0_0", "window_timestamped");
        createWindowStore("1_2", "window_timestamped");
        final var plain = createWindowStore("0_0", "window_plain");
        final var keyValue = createKeyValueStore("0_0", "key_value");
        final var otherCheckpoint = createCheckpoint("2_0");
        createCheckpoint("0_0");
        createCheckpoint("1_2");
        // Not a task directory
        final var notATask = createWindowStore("other", "window_timestamped");

        prepare(List.of(
                windowStore("window_timestamped", true, true, true),
                // A plain store in KSML 1.x: Kafka Streams converts it itself
                windowStore("window_plain", true, null, true),
                keyValueStore("key_value", true, false)));

        for (final var task : List.of("0_0", "1_2")) {
            assertThat(applicationDirectory().resolve(task).resolve("window_timestamped")).doesNotExist();
            assertThat(applicationDirectory().resolve(task).resolve("window_timestamped.position")).doesNotExist();
            assertThat(applicationDirectory().resolve(task).resolve(".checkpoint")).doesNotExist();
        }
        assertThat(plain).exists();
        assertThat(keyValue).exists();
        assertThat(otherCheckpoint).exists();
        assertThat(notATask).exists();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("deletes the checkpoint before the stores, so a stop halfway leaves no old offsets for an empty store")
    void deletesCheckpointFirst() throws IOException {
        final var store = createWindowStore("0_0", "window_timestamped");
        final var checkpoint = createCheckpoint("0_0");
        final var segment = store.resolve("window_timestamped.1794319200000");
        final var stores = List.of(windowStore("window_timestamped", true, true, true));
        // A segment that can't be deleted stops the delete halfway, as a crash would
        assumeTrue(Files.getFileStore(segment).supportsFileAttributeView(PosixFileAttributeView.class));
        final var permissions = Files.getPosixFilePermissions(segment);
        Files.setPosixFilePermissions(segment, PosixFilePermissions.fromString("r-x------"));
        try {
            assumeTrue(!Files.isWritable(segment), "the user can delete files from a read-only directory");

            assertThatThrownBy(() -> prepare(stores)).isInstanceOf(ExecutionException.class);
            assertThat(checkpoint).doesNotExist();
            assertThat(store).exists();
            assertThat(marker()).doesNotExist();
        } finally {
            Files.setPosixFilePermissions(segment, permissions);
        }

        prepare(stores);

        assertThat(store).doesNotExist();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("stops for a persistent store with local state that leaves out logging, and changes nothing")
    void stopsForUnloggedState() throws IOException {
        final var keyValue = createKeyValueStore("0_1", "key_value");
        final var window = createWindowStore("0_0", "window_plain");
        final var restorable = createWindowStore("0_0", "window_timestamped");
        final var checkpoint = createCheckpoint("0_0");
        final var stores = List.of(
                keyValueStore("key_value", true, null),
                windowStore("window_plain", true, null, null),
                windowStore("window_timestamped", true, true, true));

        assertThatThrownBy(() -> prepare(stores))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Store 'key_value' has local state in task 0_1")
                .hasMessageContaining("Store 'window_plain' has local state in task 0_0")
                .hasMessageContaining("logging: false")
                .hasMessageNotContaining("window_timestamped");
        assertThat(keyValue).exists();
        assertThat(window).exists();
        assertThat(restorable).exists();
        assertThat(checkpoint).exists();
        assertThat(marker()).doesNotExist();
    }

    @Test
    @DisplayName("keeps the local state of a store that sets logging explicitly")
    void keepsStateWithExplicitLogging() throws IOException {
        final var unlogged = createKeyValueStore("0_0", "key_value_unlogged");
        final var logged = createKeyValueStore("0_0", "key_value_logged");

        prepare(List.of(keyValueStore("key_value_unlogged", true, false), keyValueStore("key_value_logged", true, true)));

        assertThat(unlogged).exists();
        assertThat(logged).exists();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("stops for a timestamped window store without a changelog, whatever logging says, and names the files to delete")
    void stopsForUnreadableState() throws IOException {
        final var unset = createWindowStore("0_0", "window_unset");
        final var unlogged = createWindowStore("1_0", "window_unlogged");
        final var stores = List.of(
                windowStore("window_unset", true, true, null),
                windowStore("window_unlogged", true, true, false));

        assertThatThrownBy(() -> prepare(stores))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Window store 'window_unset' has local state in task 0_0")
                .hasMessageContaining("Window store 'window_unlogged' has local state in task 1_0")
                .hasMessageContaining(unset.toString())
                .hasMessageContaining(unlogged + ".position");
        assertThat(unset).exists();
        assertThat(unlogged).exists();
        assertThat(marker()).doesNotExist();
    }

    @Test
    @DisplayName("starts once the user has deleted the state that the check named")
    void startsAfterTheStateIsDeleted() throws IOException {
        createWindowStore("0_0", "window_unlogged");
        final var stores = List.of(windowStore("window_unlogged", true, true, false));
        assertThatThrownBy(() -> prepare(stores)).isInstanceOf(ExecutionException.class);

        Files.delete(applicationDirectory().resolve("0_0").resolve("window_unlogged.position"));
        try (var files = Files.walk(applicationDirectory().resolve("0_0").resolve("window_unlogged"))) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
        prepare(stores);

        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("with exactly-once, stops when clearing a window store would delete a task with a store that keeps its state")
    void stopsWhenExactlyOnceWipesKeptState() throws IOException {
        final var window = createWindowStore("0_0", "window_timestamped");
        final var kept = createKeyValueStore("0_0", "key_value_unlogged");
        final var checkpoint = createCheckpoint("0_0");
        final var stores = List.of(windowStore("window_timestamped", true, true, true), keyValueStore("key_value_unlogged", true, false));

        assertThatThrownBy(() -> prepareWithExactlyOnce(stores))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Task 0_0: KSML clears window store 'window_timestamped'")
                .hasMessageContaining("'key_value_unlogged' with 'logging: false'")
                .hasMessageContaining("processing.guarantee: at_least_once");
        assertThat(window).exists();
        assertThat(kept).exists();
        assertThat(checkpoint).exists();
        assertThat(marker()).doesNotExist();
    }

    @Test
    @DisplayName("with exactly-once, clears a window store when no store of its task keeps its state")
    void clearsWithExactlyOnceWithoutKeptStateInTask() throws IOException {
        final var window = createWindowStore("0_0", "window_timestamped");
        final var logged = createKeyValueStore("0_0", "key_value_logged");
        final var otherTask = createKeyValueStore("1_0", "key_value_unlogged");

        prepareWithExactlyOnce(List.of(
                windowStore("window_timestamped", true, true, true),
                keyValueStore("key_value_logged", true, true),
                keyValueStore("key_value_unlogged", true, false)));

        assertThat(window).doesNotExist();
        assertThat(logged).exists();
        assertThat(otherTask).exists();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("with at-least-once, clears a window store next to a store that keeps its state")
    void clearsWithAtLeastOnce() throws IOException {
        final var window = createWindowStore("0_0", "window_timestamped");
        final var kept = createKeyValueStore("0_0", "key_value_unlogged");

        prepare(List.of(windowStore("window_timestamped", true, true, true), keyValueStore("key_value_unlogged", true, false)));

        assertThat(window).doesNotExist();
        assertThat(kept).exists();
    }

    @ParameterizedTest(name = "thisStore logging: {0}, otherStore logging: {1}")
    @CsvSource(nullValues = "unset", value = {"unset, unset", "false, unset", "unset, false", "false, true"})
    @DisplayName("stops for the shared store of a join that had no changelog in KSML 1.x and gets one now, and changes nothing")
    void stopsForUnloggedSharedJoinState(Boolean thisLogging, Boolean otherLogging) throws IOException {
        final var shared = createKeyValueStore("0_0", SHARED_JOIN_STORE);
        final var checkpoint = createCheckpoint("0_0");
        final var sharedJoinStores = List.of(sharedJoinStore(true, thisLogging, otherLogging));

        assertThatThrownBy(() -> prepare(List.of(), sharedJoinStores))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("The shared store '" + SHARED_JOIN_STORE + "' of the join of 'this_store' and 'other_store' has local state in task 0_0")
                .hasMessageContaining("Set 'logging: false' on both 'this_store' and 'other_store'")
                .hasMessageContaining("'logging: true' on 'this_store'");
        assertThat(shared).exists();
        assertThat(checkpoint).exists();
        assertThat(marker()).doesNotExist();
    }

    @ParameterizedTest(name = "thisStore logging: {0}, otherStore logging: {1}")
    @CsvSource(nullValues = "unset", value = {
            // Logged in KSML 1.x
            "true, unset",
            // Not logged now either
            "false, false"})
    @DisplayName("keeps the local state of a shared join store whose changelog does not change")
    void keepsSharedJoinStateWithoutNewChangelog(Boolean thisLogging, Boolean otherLogging) throws IOException {
        final var shared = createKeyValueStore("0_0", SHARED_JOIN_STORE);

        prepare(List.of(), List.of(sharedJoinStore(true, thisLogging, otherLogging)));

        assertThat(shared).exists();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("marks the state directory of an application whose only persistent stores are join stores")
    void marksApplicationWithOnlyJoinStores() throws IOException {
        Files.createDirectories(applicationDirectory().resolve("0_0"));

        prepare(List.of(), List.of(sharedJoinStore(true, null, null)));

        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("with exactly-once, stops when clearing a window store would delete a task with a shared join store that keeps its state")
    void stopsWhenExactlyOnceWipesSharedJoinState() throws IOException {
        final var window = createWindowStore("0_0", "window_timestamped");
        final var shared = createKeyValueStore("0_0", SHARED_JOIN_STORE);

        assertThatThrownBy(() -> prepareWithExactlyOnce(List.of(windowStore("window_timestamped", true, true, true)), List.of(sharedJoinStore(true, false, false))))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Task 0_0: KSML clears window store 'window_timestamped'")
                .hasMessageContaining("'" + SHARED_JOIN_STORE + "' with 'logging: false'");
        assertThat(window).exists();
        assertThat(shared).exists();
        assertThat(marker()).doesNotExist();
    }

    @Test
    @DisplayName("checks only the window stores of Python functions for the KSML 1.x format")
    void checksOnlyPythonWindowStoresForTheFormat() throws IOException {
        final var dslStore = createWindowStore("0_0", "window_dsl");
        final var store = windowStore("window_dsl", true, true, true);

        StoreUpgrade.prepare(applicationDirectory(), List.of(store), List.of(), List.of(), false);

        assertThat(dslStore).exists();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("ignores stores without local state and in-memory stores")
    void ignoresStoresWithoutState() throws IOException {
        Files.createDirectories(applicationDirectory().resolve("0_0"));

        prepare(List.of(
                keyValueStore("key_value", true, null),
                windowStore("window_timestamped", true, true, null),
                keyValueStore("key_value_memory", false, null)));

        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("ignores stores whose RocksDB files hold no data, as KSML 1.3.x leaves them")
    void ignoresStoresWithoutData() throws IOException {
        final var task = applicationDirectory().resolve("0_0");
        final var keyValue = createEmptyRocksDb(task.resolve("rocksdb").resolve("key_value"));
        final var window = createEmptyRocksDb(task.resolve("window_plain").resolve("window_plain.1794319200000"));
        final var unreadable = createEmptyRocksDb(task.resolve("window_unlogged").resolve("window_unlogged.1794319200000"));
        final var shared = createEmptyRocksDb(task.resolve("rocksdb").resolve(SHARED_JOIN_STORE));

        prepare(List.of(
                        keyValueStore("key_value", true, null),
                        windowStore("window_plain", true, null, null),
                        windowStore("window_unlogged", true, true, false)),
                List.of(sharedJoinStore(true, null, null)));

        assertThat(keyValue).exists();
        assertThat(window).exists();
        assertThat(unreadable).exists();
        assertThat(shared).exists();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("with exactly-once, clears a window store next to a store that keeps its state but has no data")
    void clearsWithExactlyOnceNextToStoreWithoutData() throws IOException {
        final var window = createWindowStore("0_0", "window_timestamped");
        final var kept = createEmptyRocksDb(applicationDirectory().resolve("0_0").resolve("rocksdb").resolve("key_value_unlogged"));

        prepareWithExactlyOnce(List.of(windowStore("window_timestamped", true, true, true), keyValueStore("key_value_unlogged", true, false)));

        assertThat(window).doesNotExist();
        assertThat(kept).exists();
        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("counts BlobDB files as data")
    void stopsForBlobData() throws IOException {
        final var keyValue = createEmptyRocksDb(applicationDirectory().resolve("0_0").resolve("rocksdb").resolve("key_value"));
        Files.writeString(keyValue.resolve("000012.blob"), "data");
        final var stores = List.of(keyValueStore("key_value", true, null));

        assertThatThrownBy(() -> prepare(stores))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Store 'key_value' has local state in task 0_0");
        assertThat(marker()).doesNotExist();
    }

    @Test
    @DisplayName("leaves a marked application directory alone, so the check runs once")
    void keepsMarkedState() throws IOException {
        final var stores = List.of(windowStore("window_timestamped", true, true, true), keyValueStore("key_value", true, null));
        createWindowStore("0_0", "window_timestamped");
        prepare(stores);

        // Kafka Streams has written the stores since the check
        final var window = createWindowStore("0_0", "window_timestamped");
        final var keyValue = createKeyValueStore("0_0", "key_value");
        final var checkpoint = createCheckpoint("0_0");
        prepare(stores);

        assertThat(window).exists();
        assertThat(applicationDirectory().resolve("0_0").resolve("window_timestamped.position")).exists();
        assertThat(keyValue).exists();
        assertThat(checkpoint).exists();
    }

    @Test
    @DisplayName("creates and marks a missing application directory")
    void marksNewApplicationDirectory() {
        prepare(List.of(keyValueStore("key_value", true, null)));

        assertThat(marker()).exists();
    }

    @Test
    @DisplayName("leaves the state directory alone without persistent stores")
    void ignoresApplicationWithoutPersistentStores() {
        prepare(List.of(windowStore("window_memory", false, true, null), keyValueStore("key_value_memory", false, null)));
        prepare(List.of());

        assertThat(applicationDirectory()).doesNotExist();
    }

    @Test
    @DisplayName("logs which stores it deletes")
    void logsDeletedStores() throws Exception {
        createWindowStore("0_0", "window_timestamped");
        final var stores = List.of(windowStore("window_timestamped", true, true, true));

        final var log = stderrOf(() -> prepare(stores));

        assertThat(log.lines().filter(line -> line.contains("INFO") && line.contains("window_timestamped"))).hasSize(1);
    }
}
