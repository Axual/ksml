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
import io.axual.ksml.type.UserType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one-time cleanup of window store state that KSML 1.x wrote in a format this version can't read
 * (AXPD-12445, design D7).
 */
class WindowStoreUpgradeTest {
    private static final UserType UNKNOWN = UserType.UNKNOWN;

    @TempDir
    Path stateDirectory;

    private static WindowStateStoreDefinition windowStore(String name, boolean persistent, boolean timestamped, boolean logging) {
        return new WindowStateStoreDefinition(name, persistent, timestamped, Duration.ofHours(1), Duration.ofMinutes(1), false, UNKNOWN, UNKNOWN, false, logging);
    }

    private static final List<StateStoreDefinition> STORES = List.of(
            windowStore("window_timestamped", true, true, true),
            windowStore("window_plain", true, false, true),
            windowStore("window_memory", false, true, true),
            new KeyValueStateStoreDefinition("key_value", true, true, false, null, null, UNKNOWN, UNKNOWN, false, true));

    private Path applicationDirectory() {
        return stateDirectory.resolve("app");
    }

    private Path createStore(String task, String store) throws IOException {
        final var storeDirectory = applicationDirectory().resolve(task).resolve(store);
        Files.createDirectories(storeDirectory.resolve(store + ".1794319200000"));
        Files.writeString(storeDirectory.resolve(store + ".1794319200000").resolve("000010.sst"), "data");
        Files.writeString(applicationDirectory().resolve(task).resolve(store + ".position"), "position");
        return storeDirectory;
    }

    private Path createCheckpoint(String task) throws IOException {
        return Files.writeString(applicationDirectory().resolve(task).resolve(".checkpoint"), "checkpoint");
    }

    @Test
    @DisplayName("deletes the persistent timestamped window stores of every task, with their position files and the checkpoint of their task")
    void deletesUnmarkedWindowStoreState() throws IOException {
        createStore("0_0", "window_timestamped");
        createStore("1_2", "window_timestamped");
        final var plain = createStore("0_0", "window_plain");
        final var keyValue = createStore("0_0", "key_value");
        final var otherKeyValue = createStore("2_0", "key_value");
        createCheckpoint("0_0");
        createCheckpoint("1_2");
        final var otherCheckpoint = createCheckpoint("2_0");
        // Not a task directory
        final var notATask = createStore("other", "window_timestamped");

        WindowStoreUpgrade.prepare(applicationDirectory(), STORES);

        for (final var task : List.of("0_0", "1_2")) {
            assertThat(applicationDirectory().resolve(task).resolve("window_timestamped")).doesNotExist();
            assertThat(applicationDirectory().resolve(task).resolve("window_timestamped.position")).doesNotExist();
            assertThat(applicationDirectory().resolve(task).resolve(".checkpoint")).doesNotExist();
        }
        assertThat(plain).exists();
        assertThat(keyValue).exists();
        assertThat(otherKeyValue).exists();
        assertThat(otherCheckpoint).exists();
        assertThat(notATask).exists();
        assertThat(applicationDirectory().resolve(WindowStoreUpgrade.MARKER_FILE)).exists();
    }

    @Test
    @DisplayName("leaves a marked application directory alone, so the cleanup runs once")
    void keepsMarkedState() throws IOException {
        createStore("0_0", "window_timestamped");
        WindowStoreUpgrade.prepare(applicationDirectory(), STORES);

        // Kafka Streams has created the store again since the cleanup
        final var store = createStore("0_0", "window_timestamped");
        final var checkpoint = createCheckpoint("0_0");
        WindowStoreUpgrade.prepare(applicationDirectory(), STORES);

        assertThat(store).exists();
        assertThat(applicationDirectory().resolve("0_0").resolve("window_timestamped.position")).exists();
        assertThat(checkpoint).exists();
    }

    @Test
    @DisplayName("creates and marks a missing application directory")
    void marksNewApplicationDirectory() {
        WindowStoreUpgrade.prepare(applicationDirectory(), STORES);

        assertThat(applicationDirectory().resolve(WindowStoreUpgrade.MARKER_FILE)).exists();
    }

    @Test
    @DisplayName("logs which stores it deletes, and warns for a store without changelog")
    void logsDeletedStores() throws Exception {
        createStore("0_0", "window_timestamped");
        createStore("0_0", "window_unlogged");
        final var stores = List.<StateStoreDefinition>of(
                windowStore("window_timestamped", true, true, true),
                windowStore("window_unlogged", true, true, false));

        final var log = stderrOf(() -> WindowStoreUpgrade.prepare(applicationDirectory(), stores));

        assertThat(applicationDirectory().resolve("0_0").resolve("window_unlogged")).doesNotExist();
        assertThat(log.lines().filter(line -> line.contains("INFO") && line.contains("window_timestamped"))).hasSize(1);
        assertThat(log.lines().filter(line -> line.contains("WARN") && line.contains("window_unlogged"))).hasSize(1);
    }

    private static String stderrOf(ThrowingRunnable action) throws Exception {
        // slf4j-simple writes to System.err
        final var originalErr = System.err;
        final var captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            action.run();
        } finally {
            System.setErr(originalErr);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
