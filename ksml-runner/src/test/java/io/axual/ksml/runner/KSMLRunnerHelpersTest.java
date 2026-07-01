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

import com.fasterxml.jackson.databind.JsonNode;
import io.axual.ksml.client.serde.ResolvingDeserializer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyDefinition;
import io.axual.ksml.generator.YAMLObjectMapper;
import io.axual.ksml.runner.backend.Runner;
import io.axual.ksml.runner.config.ApplicationServerConfig;
import io.axual.ksml.runner.config.ErrorHandlingConfig;
import io.axual.ksml.runner.config.KSMLConfig;
import io.axual.ksml.runner.config.KSMLRunnerConfig;
import io.axual.ksml.runner.config.NotationConfig;
import io.axual.ksml.runner.config.NotationConfig.NotationType;
import io.axual.ksml.runner.config.SchemaRegistryConfig;
import io.axual.ksml.runner.config.internal.StringMap;
import io.axual.ksml.runner.exception.ConfigException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the pure helpers extracted from {@link KSMLRunner#main(String[])}: notation config
 * merging, definition splitting and error-handler wiring. These cover the branch-heavy logic that
 * used to be buried inside {@code main}.
 */
class KSMLRunnerHelpersTest {

    // ---- mergeNotationConfigs ------------------------------------------------------------------

    private static StringMap stringMap(String key, String value) {
        final var map = new StringMap();
        map.put(key, value);
        return map;
    }

    @Test
    @DisplayName("Without a schema registry or config the merged notation config is empty")
    void mergeWithoutRegistryOrConfig() {
        final var notationConfig = NotationConfig.builder().type(NotationType.JSON).build();

        assertThat(KSMLRunner.mergeNotationConfigs(notationConfig, Map.of())).isEmpty();
    }

    @Test
    @DisplayName("A referenced schema registry's config is included in the merged notation config")
    void mergeIncludesSchemaRegistryConfig() {
        final var srConfig = SchemaRegistryConfig.builder().config(stringMap("schema.registry.url", "http://sr:8081")).build();
        final var notationConfig = NotationConfig.builder().type(NotationType.CONFLUENT_AVRO).schemaRegistry("primary").build();

        final var merged = KSMLRunner.mergeNotationConfigs(notationConfig, Map.of("primary", srConfig));

        assertThat(merged).containsEntry("schema.registry.url", "http://sr:8081");
    }

    @Test
    @DisplayName("A missing schema registry reference is ignored and yields no config")
    void mergeIgnoresUnknownSchemaRegistry() {
        final var notationConfig = NotationConfig.builder().type(NotationType.CONFLUENT_AVRO).schemaRegistry("does-not-exist").build();

        // schemaRegistries map does not contain the referenced name -> warn branch, empty result.
        assertThat(KSMLRunner.mergeNotationConfigs(notationConfig, Map.of())).isEmpty();
    }

    @Test
    @DisplayName("A schema registry entry without config contributes nothing")
    void mergeHandlesRegistryWithoutConfig() {
        final var srConfig = SchemaRegistryConfig.builder().config(null).build();
        final var notationConfig = NotationConfig.builder().type(NotationType.CONFLUENT_AVRO).schemaRegistry("primary").build();

        assertThat(KSMLRunner.mergeNotationConfigs(notationConfig, Map.of("primary", srConfig))).isEmpty();
    }

    @Test
    @DisplayName("Notation-level config is merged on top of the schema registry config")
    void mergeNotationConfigOverridesRegistryConfig() {
        final var srConfig = SchemaRegistryConfig.builder()
                .config(stringMap("shared.key", "from-registry"))
                .build();
        final var notationConfig = NotationConfig.builder()
                .type(NotationType.CONFLUENT_AVRO)
                .schemaRegistry("primary")
                .config(stringMap("shared.key", "from-notation"))
                .build();

        final var merged = KSMLRunner.mergeNotationConfigs(notationConfig, Map.of("primary", srConfig));

        // The notation's own config is applied last, so it wins on key collisions.
        assertThat(merged).containsEntry("shared.key", "from-notation");
    }

    // ---- splitDefinitions ----------------------------------------------------------------------

    private static TopologyDefinition definitionWith(boolean hasProducers, boolean hasPipelines) {
        final var def = mock(TopologyDefinition.class);
        when(def.producers()).thenReturn(hasProducers ? Map.of("p", mock(ProducerDefinition.class)) : Map.of());
        when(def.pipelines()).thenReturn(hasPipelines ? Map.of("l", mock(PipelineDefinition.class)) : Map.of());
        return def;
    }

    @Test
    @DisplayName("A definition is routed to producers and/or pipelines based on its content")
    void splitRoutesDefinitionsByContent() {
        final var producerOnly = definitionWith(true, false);
        final var pipelineOnly = definitionWith(false, true);
        final var both = definitionWith(true, true);

        final var split = KSMLRunner.splitDefinitions(
                Map.of("producerOnly", producerOnly, "pipelineOnly", pipelineOnly, "both", both),
                true, true);

        assertThat(split.producers()).containsOnlyKeys("producerOnly", "both");
        assertThat(split.pipelines()).containsOnlyKeys("pipelineOnly", "both");
    }

    @Test
    @DisplayName("Disabling producers drops producer definitions but keeps pipelines")
    void splitHonoursDisabledProducers() {
        final var both = definitionWith(true, true);

        final var split = KSMLRunner.splitDefinitions(Map.of("both", both), false, true);

        assertThat(split.producers()).isEmpty();
        assertThat(split.pipelines()).containsOnlyKeys("both");
    }

    @Test
    @DisplayName("Disabling pipelines drops pipeline definitions but keeps producers")
    void splitHonoursDisabledPipelines() {
        final var both = definitionWith(true, true);

        final var split = KSMLRunner.splitDefinitions(Map.of("both", both), true, false);

        assertThat(split.producers()).containsOnlyKeys("both");
        assertThat(split.pipelines()).isEmpty();
    }

    @Test
    @DisplayName("Splitting an empty definition set yields two empty maps")
    void splitEmptyDefinitions() {
        final var split = KSMLRunner.splitDefinitions(Map.of(), true, true);

        assertThat(split.producers()).isEmpty();
        assertThat(split.pipelines()).isEmpty();
    }

    // ---- validateDefinitions -------------------------------------------------------------------

    @Test
    @DisplayName("validateDefinitions rejects a null or empty definition set")
    void validateDefinitionsRejectsEmpty() {
        assertThatThrownBy(() -> KSMLRunner.validateDefinitions(null)).isInstanceOf(ConfigException.class);
        assertThatThrownBy(() -> KSMLRunner.validateDefinitions(Map.of())).isInstanceOf(ConfigException.class);
    }

    @Test
    @DisplayName("validateDefinitions accepts a non-empty definition set")
    void validateDefinitionsAcceptsNonEmpty() {
        assertThatCode(() -> KSMLRunner.validateDefinitions(Map.of("ns", new Object()))).doesNotThrowAnyException();
    }

    // ---- registerNotations ---------------------------------------------------------------------

    @Test
    @DisplayName("registerNotations registers the built-in notations and the AVRO fallback when none are configured")
    void registerNotationsRegistersDefaultsAndAvroFallback() {
        final var ksmlConfig = new KSMLConfig(); // no notation overrides configured

        KSMLRunner.registerNotations(ksmlConfig, Map.of());

        final var library = ExecutionContext.INSTANCE.notationLibrary();
        assertThat(library.getIfExists(JsonNotation.NOTATION_NAME)).as("json default").isNotNull();
        assertThat(library.getIfExists(BinaryNotation.NOTATION_NAME)).as("binary default").isNotNull();
        // With no notations configured, the Confluent AVRO fallback is registered.
        assertThat(library.getIfExists("avro")).as("avro fallback").isNotNull();
    }

    @Test
    @DisplayName("registerNotations registers a configured notation override under its name")
    void registerNotationsRegistersConfiguredOverride() {
        final var notations = new KSMLConfig.NotationMap();
        notations.add("myJson", NotationConfig.builder().type(NotationType.JSON).build());
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.notations(notations);

        KSMLRunner.registerNotations(ksmlConfig, Map.of());

        assertThat(ExecutionContext.INSTANCE.notationLibrary().getIfExists("myJson")).isNotNull();
    }

    @Test
    @DisplayName("registerNotations skips a notation override whose type is missing")
    void registerNotationsSkipsOverrideWithoutType() {
        final var notations = new KSMLConfig.NotationMap();
        notations.add("incomplete", NotationConfig.builder().type(null).build()); // no type -> incomplete
        final var ksmlConfig = new KSMLConfig();
        ksmlConfig.notations(notations);

        // The incomplete entry is logged and skipped; no exception, and it is not registered.
        assertThatCode(() -> KSMLRunner.registerNotations(ksmlConfig, Map.of())).doesNotThrowAnyException();
        assertThat(ExecutionContext.INSTANCE.notationLibrary().getIfExists("incomplete")).isNull();
    }

    // ---- wrapSerde -----------------------------------------------------------------------------

    @Test
    @DisplayName("wrapSerde wraps a serde in resolving (de)serializers")
    void wrapSerdeWrapsInResolvingSerdes() {
        final var wrapped = KSMLRunner.wrapSerde(Serdes.String(), Map.of("bootstrap.servers", "localhost:9092"));

        assertThat(wrapped.serializer()).isInstanceOf(ResolvingSerializer.class);
        assertThat(wrapped.deserializer()).isInstanceOf(ResolvingDeserializer.class);
    }

    // ---- parseDefinitions ----------------------------------------------------------------------

    @Test
    @DisplayName("parseDefinitions parses each raw definition into a TopologyDefinition")
    void parseDefinitionsParsesEachNamespace() throws Exception {
        final JsonNode definition = YAMLObjectMapper.INSTANCE.readValue(
                """
                streams:
                  some_stream:
                    topic: some_topic
                    keyType: string
                    valueType: string
                """, JsonNode.class);

        final var parsed = KSMLRunner.parseDefinitions(Map.of("ns", definition));

        assertThat(parsed).containsKey("ns");
        assertThat(parsed.get("ns")).isNotNull();
    }

    // ---- stopRunnersQuietly --------------------------------------------------------------------

    @Test
    @DisplayName("stopRunnersQuietly stops both runners when present")
    void stopRunnersQuietlyStopsBoth() {
        final var producer = mock(Runner.class);
        final var streams = mock(Runner.class);

        KSMLRunner.stopRunnersQuietly(producer, streams);

        verify(producer).stop();
        verify(streams).stop();
    }

    @Test
    @DisplayName("stopRunnersQuietly tolerates a null runner and a failing stop")
    void stopRunnersQuietlyToleratesNullAndExceptions() {
        final var failing = mock(Runner.class);
        doThrow(new RuntimeException("stop failed")).when(failing).stop();

        // Null streams must not NPE, and an exception from stop() must be swallowed.
        assertThatCode(() -> KSMLRunner.stopRunnersQuietly(failing, null)).doesNotThrowAnyException();
        verify(failing).stop();
    }

    // ---- stopOtherOnFailure --------------------------------------------------------------------

    @Test
    @DisplayName("stopOtherOnFailure stops the other runner when a failure occurred")
    void stopOtherOnFailureStopsOther() {
        final var other = mock(Runner.class);

        KSMLRunner.stopOtherOnFailure(new RuntimeException("boom"), "Producer failed", other);

        verify(other).stop();
    }

    @Test
    @DisplayName("stopOtherOnFailure does nothing on success")
    void stopOtherOnFailureNoopOnSuccess() {
        final var other = mock(Runner.class);

        KSMLRunner.stopOtherOnFailure(null, "Producer failed", other);

        verify(other, never()).stop();
    }

    @Test
    @DisplayName("stopOtherOnFailure tolerates a null other runner on failure")
    void stopOtherOnFailureToleratesNullOther() {
        assertThatCode(() -> KSMLRunner.stopOtherOnFailure(new RuntimeException("boom"), "Producer failed", null))
                .doesNotThrowAnyException();
    }

    // ---- runner / rest-server factories --------------------------------------------------------

    private static KSMLRunnerConfig runnerConfig() {
        final var config = new KSMLRunnerConfig();
        config.setKafkaConfig(new KSMLRunnerConfig.KafkaConfig());
        config.setKsmlConfig(new KSMLConfig());
        return config;
    }

    @Test
    @DisplayName("createProducerRunner returns null when there are no producer definitions")
    void createProducerRunnerNullWhenEmpty() {
        assertThat(KSMLRunner.createProducerRunner(Map.of(), runnerConfig(), new KSMLConfig())).isNull();
    }

    @Test
    @DisplayName("createProducerRunner builds a runner when producer definitions are present")
    void createProducerRunnerBuildsRunner() {
        final var definitions = Map.of("ns", mock(TopologyDefinition.class));

        final var runner = KSMLRunner.createProducerRunner(definitions, runnerConfig(), new KSMLConfig());

        assertThat(runner).isNotNull();
        assertThat(runner.getState()).isEqualTo(Runner.State.CREATED);
    }

    @Test
    @DisplayName("createStreamsRunner returns null when there are no pipeline definitions")
    void createStreamsRunnerNullWhenEmpty() {
        assertThat(KSMLRunner.createStreamsRunner(Map.of(), runnerConfig(), new KSMLConfig())).isNull();
    }

    @Test
    @DisplayName("createRestServer returns null when the application server is disabled")
    void createRestServerNullWhenDisabled() {
        // A default ApplicationServerConfig is disabled, so no server is started.
        assertThat(KSMLRunner.createRestServer(new ApplicationServerConfig())).isNull();
    }

    // ---- setupErrorHandling --------------------------------------------------------------------

    private static ErrorHandler reflectHandler(String fieldName) throws Exception {
        final Field field = ExecutionContext.INSTANCE.errorHandling().getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (ErrorHandler) field.get(ExecutionContext.INSTANCE.errorHandling());
    }

    @Test
    @DisplayName("A null error-handling config leaves the handlers untouched")
    void setupErrorHandlingToleratesNull() {
        assertThatCode(() -> KSMLRunner.setupErrorHandling(null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Error handling wires consume, produce and process handlers with their default loggers")
    void setupErrorHandlingWiresAllChannels() throws Exception {
        KSMLRunner.setupErrorHandling(new ErrorHandlingConfig());

        // All three channels are wired from the config, each with its documented default logger name.
        assertThat(reflectHandler("consumeHandler").loggerName()).isEqualTo("ConsumeError");
        assertThat(reflectHandler("produceHandler").loggerName()).isEqualTo("ProduceError");
        assertThat(reflectHandler("processHandler").loggerName()).isEqualTo("ProcessError");
    }
}
