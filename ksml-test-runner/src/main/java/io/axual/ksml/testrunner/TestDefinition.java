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
 * The top-level test definition parsed from a YAML test file.
 *
 * @param name            human-readable test name
 * @param pipeline        classpath-relative path to the KSML pipeline definition YAML
 * @param schemaDirectory optional path to Avro schema files
 * @param produce         list of produce blocks defining test data
 * @param assertions      list of assertion blocks defining expected outcomes
 */
@JsonSchema(description = "KSML test definition")
public record TestDefinition(
        @JsonSchema(description = "Human-readable test name", required = true,
                examples = {"Filter keeps only blue sensors"})
        String name,

        @JsonSchema(description = "Path to the KSML pipeline definition YAML", required = true,
                examples = {"pipelines/my-pipeline.yaml"})
        String pipeline,

        @JsonSchema(description = "Path to Avro schema files directory", examples = {"schemas"})
        String schemaDirectory,

        @JsonSchema(description = "Path to externalized Python modules", examples = {"schemas"})
        String moduleDirectory,

        @JsonSchema(description = "Registry entries mapping topics to their key/value types for mock schema registry population")
        List<RegistryEntry> registry,

        @JsonSchema(description = "List of produce blocks defining test data to send to input topics", required = true)
        List<ProduceBlock> produce,

        @JsonSchema(description = "List of assertion blocks defining expected outcomes", required = true)
        List<AssertBlock> assertions
) {
}
