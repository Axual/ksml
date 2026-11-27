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

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * A block that defines test data to be produced into a stream declared in the
 * suite's {@code streams:} map.
 *
 * @param to        key into the suite's {@code streams:} map identifying the target stream
 * @param messages  inline test messages, empty if absent (mutually exclusive with {@code generator})
 * @param generator generator function definition as a map (KSML generator syntax), empty if absent
 * @param count     optional count for generator-based production
 */
@JsonSchema(
        description = "A block that defines test data to be produced into a stream",
        anyOfRequired = {KSMLTestDSL.Produce.MESSAGES, KSMLTestDSL.Produce.GENERATOR, KSMLTestDSL.Produce.ADVANCE_WALL_CLOCK_TIME},
        mutuallyExclusive = {KSMLTestDSL.Produce.MESSAGES, KSMLTestDSL.Produce.GENERATOR})
public record ProduceBlock(
        @JsonSchema(yamlName = KSMLTestDSL.Produce.TO,
                description = "Key into the suite's streams: map identifying the target stream",
                required = true,
                examples = {"sensor_source", "raw_input"})
        String to,

        @JsonSchema(yamlName = KSMLTestDSL.Produce.MESSAGES,
                description = "Inline test messages to produce")
        List<TestMessage> messages,

        @JsonSchema(yamlName = KSMLTestDSL.Produce.GENERATOR,
                description = "Generator function definition (KSML generator syntax)")
        Map<String, Object> generator,

        @JsonSchema(yamlName = KSMLTestDSL.Produce.COUNT,
                description = "Number of times to invoke the generator function")
        Long count,

        @JsonSchema(yamlName = KSMLTestDSL.Produce.ADVANCE_WALL_CLOCK_TIME,
                description = "Advances the wall-clock time of the test driver by this KSML duration after producing the block's messages, so wall-clock punctuations that become due run",
                examples = {"1m", "30s"})
        Duration advanceWallClockTime
) {
    /** A produce block that does not advance the wall-clock time. */
    public ProduceBlock(String to, List<TestMessage> messages, Map<String, Object> generator, Long count) {
        this(to, messages, generator, count, null);
    }

    /**
     * Normalizes {@code messages}/{@code generator} to an empty collection when constructed with
     * {@code null}, so every other method on this record can treat them as never-null.
     */
    public ProduceBlock {
        messages = messages != null ? messages : List.of();
        generator = generator != null ? generator : Map.of();
    }

    /**
     * Validate that the produce block has messages, a generator or advanceWallClockTime, and not both messages and a
     * generator.
     */
    public void validate() {
        // A block that only advances the wall-clock time needs no messages
        if (messages.isEmpty() && generator.isEmpty() && advanceWallClockTime == null) {
            throw new TestDefinitionException(
                    "Produce block targeting stream '" + to + "' must have '"
                            + KSMLTestDSL.Produce.MESSAGES + "', '" + KSMLTestDSL.Produce.GENERATOR + "' or '"
                            + KSMLTestDSL.Produce.ADVANCE_WALL_CLOCK_TIME + "'");
        }
        if (!messages.isEmpty() && !generator.isEmpty()) {
            throw new TestDefinitionException(
                    "Produce block targeting stream '" + to + "' must have either '"
                            + KSMLTestDSL.Produce.MESSAGES + "' or '" + KSMLTestDSL.Produce.GENERATOR + "', not both");
        }
    }
}
