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

/**
 * A block that defines assertions to run against pipeline output.
 * Either {@code on} (referencing a stream) or {@code stores} (a list of state store names)
 * MUST be present.
 *
 * @param on     optional key into the suite's {@code streams:} map identifying the output stream;
 *               when set, output records become a {@code records} Python list in the assertion code
 * @param stores optional list of state store names to inject as Python globals into the assertion code
 * @param code   Python assertion code to execute
 */
@JsonSchema(
        description = "A block that defines assertions to run against pipeline output",
        anyOfRequired = {"on", "stores"})
public record AssertBlock(
        @JsonSchema(description = "Key into the suite's streams: map identifying the output stream to read records from",
                examples = {"sensor_filtered", "alerts"})
        String on,

        @JsonSchema(description = "State store names to inject into the Python assertion context",
                examples = {"my_store"})
        List<String> stores,

        @JsonSchema(description = "Python assertion code to execute", required = true)
        String code
) {
    /**
     * Validate that the assert block has at least an {@code on} reference or {@code stores}.
     */
    public void validate() {
        if (on == null && stores == null) {
            throw new TestDefinitionException("Assert block must have at least 'on' or 'stores'");
        }
    }
}
