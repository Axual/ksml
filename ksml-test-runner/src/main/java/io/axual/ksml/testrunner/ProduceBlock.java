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
 * A block that defines test data to be produced into a stream declared in the
 * suite's {@code streams:} map.
 *
 * @param to        key into the suite's {@code streams:} map identifying the target stream
 * @param messages  inline test messages (mutually exclusive with {@code generator})
 * @param generator optional generator function definition as a map (KSML generator syntax)
 * @param count     optional count for generator-based production
 */
@JsonSchema(
        description = "A block that defines test data to be produced into a stream",
        oneOfRequired = {"messages", "generator"})
public record ProduceBlock(
        @JsonSchema(description = "Key into the suite's streams: map identifying the target stream",
                required = true,
                examples = {"sensor_source", "raw_input"})
        String to,

        @JsonSchema(description = "Inline test messages to produce")
        List<TestMessage> messages,

        @JsonSchema(description = "Generator function definition (KSML generator syntax)")
        Map<String, Object> generator,

        @JsonSchema(description = "Number of times to invoke the generator function")
        Long count
) {
    /**
     * Validate that the produce block has either messages or a generator, but not both.
     */
    public void validate() {
        if (messages == null && generator == null) {
            throw new TestDefinitionException(
                    "Produce block targeting stream '" + to + "' must have either 'messages' or 'generator'");
        }
        if (messages != null && generator != null) {
            throw new TestDefinitionException(
                    "Produce block targeting stream '" + to + "' must have either 'messages' or 'generator', not both");
        }
    }
}
