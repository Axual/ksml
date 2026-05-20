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
 * One hermetic test entry in a test suite. Each entry describes the produce data and
 * assertions for a single test against the suite's shared pipeline. Test entries live
 * under the suite's {@code tests:} map keyed by stable identifier; the description
 * field is optional and falls back to the key for display.
 *
 * @param description optional human-readable description; falls back to the test key
 * @param produce     produce blocks for this test
 * @param assertions  assertion blocks for this test
 */
@JsonSchema(description = "One hermetic test entry in a test suite")
public record TestCaseDefinition(
        @JsonSchema(description = "Optional human-readable description; falls back to the test key when absent",
                examples = {"Valid sensor data is transformed"})
        String description,

        @JsonSchema(description = "Produce blocks defining test data for this test", required = true)
        List<ProduceBlock> produce,

        @JsonSchema(description = "Assertion blocks defining expected outcomes for this test",
                required = true, yamlName = "assert")
        List<AssertBlock> assertions
) {
}
