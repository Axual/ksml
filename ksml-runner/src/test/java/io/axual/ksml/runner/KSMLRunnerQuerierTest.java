package io.axual.ksml.runner;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.rest.server.ComponentState;
import io.axual.ksml.runner.backend.KafkaProducerRunner;
import io.axual.ksml.runner.backend.KafkaStreamsRunner;
import io.axual.ksml.runner.backend.Runner;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the static helpers in {@link KSMLRunner} that can be exercised without bootstrapping a
 * full Kafka/streams runtime: the read-only querier and the command-line argument parsing.
 */
class KSMLRunnerQuerierTest {

    @Test
    @DisplayName("The querier reports NOT_APPLICABLE and empty results when no runners are present")
    void querierWithoutRunnersIsInert() {
        // Both runners are null, mirroring a runner started with neither pipelines nor producers.
        final var querier = KSMLRunner.getQuerier(null, null);

        assertThat(querier.getStreamRunnerState()).isEqualTo(ComponentState.NOT_APPLICABLE);
        assertThat(querier.getProducerState()).isEqualTo(ComponentState.NOT_APPLICABLE);
        assertThat(querier.allMetadataForStore("any-store")).isEmpty();
        assertThat(querier.queryMetadataForKey("any-store", "key", null)).isNull();
        assertThat(querier.<Object>store(null)).isNull();
    }

    @ParameterizedTest(name = "runner state {0} -> component state {1}")
    @DisplayName("Each runner state is translated to the matching component state")
    @CsvSource({
            "CREATED,  CREATED",
            "STARTING, STARTING",
            "STARTED,  STARTED",
            "STOPPING, STOPPING",
            "STOPPED,  STOPPED",
            "FAILED,   FAILED"
    })
    void translatesRunnerStateToComponentState(Runner.State runnerState, ComponentState expected) {
        final var streamsRunner = mock(KafkaStreamsRunner.class);
        when(streamsRunner.getState()).thenReturn(runnerState);
        final var producerRunner = mock(KafkaProducerRunner.class);
        when(producerRunner.getState()).thenReturn(runnerState);

        final var querier = KSMLRunner.getQuerier(streamsRunner, producerRunner);

        assertThat(querier.getStreamRunnerState()).isEqualTo(expected);
        assertThat(querier.getProducerState()).isEqualTo(expected);
    }

    @Test
    @DisplayName("Store queries are delegated to the underlying Kafka Streams instance")
    void delegatesStoreQueriesToKafkaStreams() {
        final var kafkaStreams = mock(KafkaStreams.class);
        final var streamsRunner = mock(KafkaStreamsRunner.class);
        when(streamsRunner.kafkaStreams()).thenReturn(kafkaStreams);

        final var metadata = List.of(mock(StreamsMetadata.class));
        when(kafkaStreams.streamsMetadataForStore("store")).thenReturn(metadata);

        final var querier = KSMLRunner.getQuerier(streamsRunner, null);

        assertThat(querier.allMetadataForStore("store")).isEqualTo(metadata);
    }

    @Test
    @DisplayName("Key metadata and store queries are delegated to the underlying Kafka Streams instance")
    void delegatesKeyMetadataAndStoreQueries() {
        final var kafkaStreams = mock(KafkaStreams.class);
        final var streamsRunner = mock(KafkaStreamsRunner.class);
        when(streamsRunner.kafkaStreams()).thenReturn(kafkaStreams);

        final var keyMetadata = mock(org.apache.kafka.streams.KeyQueryMetadata.class);
        final var serializer = new org.apache.kafka.common.serialization.StringSerializer();
        when(kafkaStreams.queryMetadataForKey("store", "key", serializer)).thenReturn(keyMetadata);

        final var querier = KSMLRunner.getQuerier(streamsRunner, null);

        assertThat(querier.queryMetadataForKey("store", "key", serializer)).isSameAs(keyMetadata);

        // store() simply forwards the StoreQueryParameters to Kafka Streams.
        final var params = org.apache.kafka.streams.StoreQueryParameters
                .fromNameAndType("store", org.apache.kafka.streams.state.QueryableStoreTypes.<String, String>keyValueStore());
        final org.apache.kafka.streams.state.ReadOnlyKeyValueStore<String, String> store = mock(org.apache.kafka.streams.state.ReadOnlyKeyValueStore.class);
        when(kafkaStreams.store(params)).thenReturn(store);
        assertThat(querier.store(params)).isSameAs(store);
    }

    @Test
    @DisplayName("populate() applies the default config file path when no arguments are supplied")
    void populateDefaultsConfigFile() {
        final var args = KSMLRunner.Arguments.populate(new String[]{});

        assertThat(args.configFile()).isEqualTo(new File("ksml-runner.yaml"));
        assertThat(args.shouldPrintKsmlSchema()).isFalse();
        assertThat(args.shouldPrintKsmlRunnerSchema()).isFalse();
    }

    @Test
    @DisplayName("populate() reads the configuration file path positional argument")
    void populateReadsConfigFileArgument() {
        final var args = KSMLRunner.Arguments.populate(new String[]{"/etc/ksml/my-runner.yaml"});

        assertThat(args.configFile()).isEqualTo(new File("/etc/ksml/my-runner.yaml"));
    }

    @Test
    @DisplayName("--schema without a value flags schema printing with no target location")
    void populateSchemaFlagWithoutLocation() {
        final var args = KSMLRunner.Arguments.populate(new String[]{"--schema"});

        assertThat(args.shouldPrintKsmlSchema()).isTrue();
        assertThat(args.ksmlSchemaLocation()).isNull();
    }

    @Test
    @DisplayName("--schema with a value flags schema printing and captures the target location")
    void populateSchemaFlagWithLocation() {
        final var args = KSMLRunner.Arguments.populate(new String[]{"--schema", "/tmp/schema.json"});

        assertThat(args.shouldPrintKsmlSchema()).isTrue();
        assertThat(args.ksmlSchemaLocation()).isEqualTo("/tmp/schema.json");
    }

    @Test
    @DisplayName("--runner-schema with a value flags runner-schema printing and captures the location")
    void populateRunnerSchemaFlagWithLocation() {
        final var args = KSMLRunner.Arguments.populate(new String[]{"--runner-schema", "/tmp/runner.json"});

        assertThat(args.shouldPrintKsmlRunnerSchema()).isTrue();
        assertThat(args.ksmlRunnerSchemaLocation()).isEqualTo("/tmp/runner.json");
        // The KSML definition schema flag remains untouched.
        assertThat(args.shouldPrintKsmlSchema()).isFalse();
    }
}
