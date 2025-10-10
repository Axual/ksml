package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.notation.json.JsonDataObjectMapper;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.notation.json.JsonSchemaMapper;
import io.axual.ksml.data.type.Flags;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonTests {
    @Test
    void schemaTest() {
        NotationTestRunner.schemaTest(JsonNotation.NOTATION_NAME, new JsonSchemaMapper(false), (input, output) -> {
            assertTrue(input.isAssignableFrom(output).isAssignable(), "Input is not assignable from the output");
            assertTrue(output.isAssignableFrom(input).isAssignable(), "Output is not assignable from the input");
        });
    }

    @Test
    void dataTest() {
        NotationTestRunner.dataTest(JsonNotation.NOTATION_NAME, new JsonDataObjectMapper(false), Flags.EMPTY);
    }

    @Test
    void serdeTest() {
        final var notation = new JsonNotation(new NotationContext(JsonNotation.NOTATION_NAME));
        NotationTestRunner.serdeTest(notation, true, new Flags());
    }
}
