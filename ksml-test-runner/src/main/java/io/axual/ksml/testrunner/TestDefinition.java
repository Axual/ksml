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
public record TestDefinition(
        String name,
        String pipeline,
        String schemaDirectory,
        List<ProduceBlock> produce,
        List<AssertBlock> assertions
) {
}
