package io.axual.ksml.runner;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KSMLRunnerTest {

    @Test
    @DisplayName("Generated runner schema emits correctly-typed defaults from @JsonProperty(defaultValue)")
    void runnerSchemaContainsTypedDefaults(@TempDir Path tempDir) throws Exception {
        final var out = tempDir.resolve("runner-spec.json");

        // main() writes the runner configuration schema and returns (no System.exit) when --runner-schema is set
        KSMLRunner.main(new String[]{"--runner-schema", out.toString()});

        assertTrue(Files.exists(out), "runner schema file should have been written");
        final var schema = Files.readString(out);

        // resolveDefaultValue converts the @JsonProperty defaultValue to the field's type:
        assertTrue(schema.contains("\"default\" : false"), "boolean defaults should be emitted as JSON booleans");
        assertTrue(schema.contains("\"default\" : true"), "boolean defaults should be emitted as JSON booleans");
        assertTrue(schema.contains("\"default\" : 9999"), "integer defaults should be emitted as JSON numbers");
        assertTrue(schema.contains("\"default\" : \"0.0.0.0\""), "string defaults should be emitted as JSON strings");
    }
}
