package io.axual.ksml.testrunner;

/*-
 * ========================LICENSE_START=================================
 * KSML Test Runner
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

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.avro.AvroNotation;
import io.axual.ksml.data.notation.avro.confluent.ConfluentAvroNotationProvider;
import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.confluent.MockConfluentSchemaRegistryClient;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Sets up the KSML execution context for test runs, registering notations
 * and schema directories. Follows the same pattern as KSMLTestExtension.
 */
@Slf4j
public class TestExecutionContext {

    @Getter
    private MockConfluentSchemaRegistryClient registryClient;

    /**
     * Initialize the execution context with all required notations, optional schema directory,
     * and topic type map for mock registry population.
     *
     * @param schemaDirectory optional path to Avro schema files, or null
     * @param topicTypeMap    merged map of topic types from registry and produce blocks
     */
    public void setup(String schemaDirectory, Map<String, RegistryEntry> topicTypeMap) {
        log.debug("Setting up test execution context");

        // Register JSON and Binary notations
        var jsonNotation = new JsonNotation(new NotationContext());
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(), jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);

        // Register Avro notation with mock schema registry under both "avro" and "confluent_avro"
        registryClient = new MockConfluentSchemaRegistryClient();
        var provider = new ConfluentAvroNotationProvider(registryClient);
        var context = new NotationContext(registryClient.configs());
        var mockAvroNotation = provider.createNotation(context);
        ExecutionContext.INSTANCE.notationLibrary().register(AvroNotation.NOTATION_NAME, mockAvroNotation);
        ExecutionContext.INSTANCE.notationLibrary().register("confluent_avro", mockAvroNotation);
        ExecutionContext.INSTANCE.notationLibrary().register("apicurio_avro", mockAvroNotation);

        // Register schema directory if provided
        if (schemaDirectory != null && !schemaDirectory.isEmpty()) {
            log.debug("Registering schema directory: {}", schemaDirectory);
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(schemaDirectory);
        } else {
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory("");
        }

        // Populate mock registry from topic type map
        if (topicTypeMap != null && schemaDirectory != null) {
            populateMockRegistry(schemaDirectory, topicTypeMap);
        }
    }

    /**
     * Populate the mock schema registry with schemas from the topic type map.
     * For each topic entry with an Avro schema reference (e.g. "avro:SensorData"),
     * loads the .avsc file from the schema directory and registers it under the
     * conventional subject name ({topic}-key or {topic}-value).
     */
    private void populateMockRegistry(String schemaDirectory, Map<String, RegistryEntry> topicTypeMap) {
        for (var entry : topicTypeMap.values()) {
            registerSchemaIfAvro(schemaDirectory, entry.topic(), entry.keyType(), true);
            registerSchemaIfAvro(schemaDirectory, entry.topic(), entry.valueType(), false);
        }
    }

    private void registerSchemaIfAvro(String schemaDirectory, String topic, String typeString, boolean isKey) {
        if (typeString == null || !typeString.contains(":")) {
            return;
        }
        var parts = typeString.split(":", 2);
        var notation = parts[0].toLowerCase();
        var schemaName = parts[1];

        // Only register Avro schemas (avro:X, confluent_avro:X, apicurio_avro:X)
        if (!notation.contains("avro")) {
            return;
        }

        var suffix = isKey ? "-key" : "-value";
        var subject = topic + suffix;
        var schemaFile = Path.of(schemaDirectory, schemaName + ".avsc");

        if (!Files.exists(schemaFile)) {
            throw new TestDefinitionException(
                    "Schema file not found for registry entry: " + schemaFile
                            + " (topic=" + topic + ", type=" + typeString + ")");
        }

        try {
            var schemaString = Files.readString(schemaFile);
            var parsedSchema = registryClient.parseSchema("AVRO", schemaString, List.of());
            if (parsedSchema.isPresent()) {
                registryClient.register(subject, parsedSchema.get());
                log.debug("Registered schema '{}' under subject '{}'", schemaName, subject);
            } else {
                throw new TestDefinitionException(
                        "Failed to parse Avro schema from " + schemaFile + " for subject " + subject);
            }
        } catch (IOException | io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException e) {
            throw new TestDefinitionException(
                    "Failed to register schema for subject '" + subject + "': " + e.getMessage(), e);
        }
    }

    /**
     * Reset the execution context for test isolation.
     */
    public void cleanup() {
        log.debug("Cleaning up test execution context");
        ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory("");
        registryClient = null;
    }
}
