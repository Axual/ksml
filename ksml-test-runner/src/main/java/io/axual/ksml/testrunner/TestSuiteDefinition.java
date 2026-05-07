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

import java.util.LinkedHashMap;

/**
 * A test suite parsed from a YAML test definition file. The file is a flat document
 * (no outer wrapper element) describing one pipeline, the streams it touches, and one
 * or more named tests that share the suite's configuration. Each test in the suite
 * runs against a fresh {@code TopologyTestDriver} for hermetic isolation.
 *
 * @param name             optional suite-level name; falls back to the filename without extension
 * @param definition       path to the KSML pipeline definition YAML
 * @param schemaDirectory  optional path to schema files
 * @param moduleDirectory  optional path to externalized Python modules
 * @param streams          named topic+type bindings, referenced from {@code to:} and {@code on:}
 * @param tests            named tests; insertion order is preserved
 */
@JsonSchema(description = "KSML test suite definition")
public record TestSuiteDefinition(
        @JsonSchema(description = "Optional suite-level name; falls back to filename without extension",
                examples = {"Filtering & transforming pipeline"})
        String name,

        @JsonSchema(description = "Path to the KSML pipeline definition YAML", required = true,
                examples = {"definitions/my-pipeline.yaml"})
        String definition,

        @JsonSchema(description = "Path to schema files directory", examples = {"schemas"})
        String schemaDirectory,

        @JsonSchema(description = "Path to externalized Python modules", examples = {"modules"})
        String moduleDirectory,

        @JsonSchema(description = "Named topic+type bindings, referenced from to:/on: in produce/assert blocks")
        LinkedHashMap<String, StreamDefinition> streams,

        @JsonSchema(description = "Named tests in the suite; at least one entry required",
                required = true, minProperties = 1)
        LinkedHashMap<String, TestCaseDefinition> tests
) {
}
