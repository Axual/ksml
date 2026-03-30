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

import java.util.List;
import java.util.Map;

/**
 * A block that defines test data to be produced to a topic.
 *
 * @param topic    the target topic name
 * @param keyType  the key type (e.g., "string")
 * @param valueType the value type (e.g., "avro:SensorData", "json")
 * @param messages inline test messages (may be null if generator is used)
 * @param generator optional generator function definition as a map (KSML generator syntax)
 * @param count    optional count for generator-based production
 */
@JsonSchema(description = "A block that defines test data to be produced to a topic")
public record ProduceBlock(
        @JsonSchema(description = "Target Kafka topic name", required = true,
                examples = {"input-topic", "sensor-data"})
        String topic,

        @JsonSchema(description = "Key serialization type", defaultValue = "string",
                examples = {"string", "avro:MyKeySchema", "json", "binary"})
        String keyType,

        @JsonSchema(description = "Value serialization type", defaultValue = "string",
                examples = {"string", "avro:SensorData", "json", "binary"})
        String valueType,

        @JsonSchema(description = "Inline test messages to produce")
        List<TestMessage> messages,

        @JsonSchema(description = "Generator function definition (KSML generator syntax)")
        Map<String, Object> generator,

        @JsonSchema(description = "Number of times to invoke the generator function")
        Long count
) {
    /**
     * Validate that the produce block has either messages or a generator.
     */
    public void validate() {
        if (messages == null && generator == null) {
            throw new TestDefinitionException(
                    "Produce block for topic '" + topic + "' must have either 'messages' or 'generator'");
        }
    }
}
