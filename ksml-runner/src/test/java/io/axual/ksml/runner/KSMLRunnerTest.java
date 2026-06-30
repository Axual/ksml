package io.axual.ksml.runner;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.runner.exception.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KSMLRunnerTest {

    @Test
    @DisplayName("Generated runner schema emits correctly-typed defaults from @JsonProperty(defaultValue)")
    void runnerSchemaContainsTypedDefaults(@TempDir Path tempDir) throws Exception {
        final var out = tempDir.resolve("runner-spec.json");

        // main() writes the runner configuration schema and returns (no System.exit) when --runner-schema is set
        KSMLRunner.main(new String[]{"--runner-schema", out.toString()});

        assertThat(out).as("runner schema file should have been written").exists();
        final var schema = Files.readString(out);

        // resolveDefaultValue converts the @JsonProperty defaultValue to the field's type, so the
        // schema must contain each default rendered with its JSON type (boolean, integer, string).
        assertThat(schema).as("typed defaults should be emitted with their JSON types").contains(
                "\"default\" : false",
                "\"default\" : true",
                "\"default\" : 9999",
                "\"default\" : \"0.0.0.0\"");
    }

    @Test
    @DisplayName("The KSML definition schema is written to file when --schema is supplied")
    void ksmlDefinitionSchemaIsWritten(@TempDir Path tempDir) throws Exception {
        final var out = tempDir.resolve("ksml-spec.json");

        // main() writes the KSML definition schema and returns when --schema is set
        KSMLRunner.main(new String[]{"--schema", out.toString()});

        assertThat(out).as("KSML definition schema file should have been written").exists();
        // The generated schema is a JSON document describing the KSML definition structure.
        assertThat(Files.readString(out)).contains("$schema");
    }

    @Test
    @DisplayName("readConfiguration parses a valid runner configuration file")
    void readConfigurationParsesValidFile() throws Exception {
        final var configFile = new File(getClass().getClassLoader().getResource("ksml-runner-config.yaml").toURI());

        final var config = KSMLRunner.readConfiguration(configFile);

        assertThat(config.getKsmlConfig()).isNotNull();
        assertThat(config.getKafkaConfigMap()).isNotNull();
    }

    @Test
    @DisplayName("readConfiguration throws a ConfigException for an unparseable file")
    void readConfigurationThrowsForInvalidFile(@TempDir Path tempDir) throws Exception {
        final var badFilePath = tempDir.resolve("bad-runner.yaml");
        Files.writeString(badFilePath, "kafka: [unterminated");
        final var badFile = badFilePath.toFile();
        assertThatThrownBy(() -> KSMLRunner.readConfiguration(badFile))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    @DisplayName("closeExecutorService shuts an idle executor down gracefully")
    void closeExecutorServiceShutsDownGracefully() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        KSMLRunner.closeExecutorService(executor);

        assertThat(executor.isShutdown()).isTrue();
        assertThat(executor.isTerminated()).as("an idle executor terminates within the grace period").isTrue();
    }

    @Test
    @DisplayName("closeExecutorService force-stops an executor that does not terminate in time")
    @SuppressWarnings("java:S2925")
    void closeExecutorServiceForcesShutdownOnTimeout() throws Exception {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final var started = new CountDownLatch(1);
        // A task that blocks past the 2s grace period forces the shutdownNow path.
        executor.submit(() -> {
            started.countDown();
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        KSMLRunner.closeExecutorService(executor);

        assertThat(executor.isShutdown()).isTrue();
    }
}
