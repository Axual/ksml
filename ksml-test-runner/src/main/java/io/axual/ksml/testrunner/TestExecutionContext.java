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

/**
 * Sets up the KSML execution context for test runs, registering notations
 * and schema directories. Follows the same pattern as KSMLTestExtension.
 */
@Slf4j
public class TestExecutionContext {

    @Getter
    private MockConfluentSchemaRegistryClient registryClient;

    /**
     * Initialize the execution context with all required notations and optional schema directory.
     *
     * @param schemaDirectory optional path to Avro schema files, or null
     */
    public void setup(String schemaDirectory) {
        log.debug("Setting up test execution context");

        // Register JSON and Binary notations
        var jsonNotation = new JsonNotation(new NotationContext());
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION,
                new BinaryNotation(new NotationContext(), jsonNotation::serde));
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);

        // Register Avro notation with mock schema registry
        registryClient = new MockConfluentSchemaRegistryClient();
        var provider = new ConfluentAvroNotationProvider(registryClient);
        var context = new NotationContext(registryClient.configs());
        var mockAvroNotation = provider.createNotation(context);
        ExecutionContext.INSTANCE.notationLibrary().register(AvroNotation.NOTATION_NAME, mockAvroNotation);

        // Register schema directory if provided
        if (schemaDirectory != null && !schemaDirectory.isEmpty()) {
            log.debug("Registering schema directory: {}", schemaDirectory);
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory(schemaDirectory);
        } else {
            ExecutionContext.INSTANCE.schemaLibrary().schemaDirectory("");
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
