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
 * A named binding from a logical stream identifier to a Kafka topic and its key/value types.
 * Stream definitions live in the suite-level {@code streams:} map and are referenced by
 * produce blocks (via {@code to:}) and assert blocks (via {@code on:}).
 *
 * @param topic     the Kafka topic name
 * @param keyType   the key type string (e.g., "string", "avro:MyKey")
 * @param valueType the value type string (e.g., "string", "avro:SensorData")
 */
@JsonSchema(description = "A named binding from a logical stream identifier to a Kafka topic and its key/value types")
public record StreamDefinition(
        @JsonSchema(description = "Kafka topic name", required = true,
                examples = {"my-topic", "sensor-data"})
        String topic,

        @JsonSchema(description = "Key serialization type", defaultValue = "string",
                examples = {"string", "avro:MyKeySchema"})
        String keyType,

        @JsonSchema(description = "Value serialization type", defaultValue = "string",
                examples = {"string", "avro:SensorData", "json"})
        String valueType
) {
    public StreamDefinition {
        if (keyType == null) keyType = "string";
        if (valueType == null) valueType = "string";
    }
}
