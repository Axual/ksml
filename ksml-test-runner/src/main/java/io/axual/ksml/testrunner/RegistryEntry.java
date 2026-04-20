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

/**
 * A registry entry that maps a topic to its key and value types for mock schema registry population.
 *
 * @param topic     the Kafka topic name
 * @param keyType   the key type (e.g., "string", "avro:MyKey")
 * @param valueType the value type (e.g., "string", "avro:SensorData")
 */
@JsonSchema(description = "A registry entry mapping a topic to its key and value types for mock schema registry population")
public record RegistryEntry(
        @JsonSchema(description = "Kafka topic name", required = true,
                examples = {"my-topic", "sensor-data"})
        String topic,

        @JsonSchema(description = "Key serialization type", defaultValue = "string",
                examples = {"string", "avro:MyKeySchema"})
        String keyType,

        @JsonSchema(description = "Value serialization type", defaultValue = "string",
                examples = {"string", "avro:SensorData", "confluent_avro"})
        String valueType
) {
    public RegistryEntry {
        if (keyType == null) keyType = "string";
        if (valueType == null) valueType = "string";
    }
}
