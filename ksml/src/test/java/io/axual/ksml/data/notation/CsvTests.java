package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.csv.CsvDataObjectMapper;
import io.axual.ksml.data.notation.csv.CsvNotation;
import io.axual.ksml.data.notation.csv.CsvSchemaMapper;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.StructSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class CsvTests {

    @Test
    void schemaTest() {
        NotationTest.schemaTest("csv", new CsvSchemaMapper(), (input, output) -> {
            // Check the conversion
            final var inputFieldNames = ((StructSchema) input).fields().stream().map(DataField::name).toArray(String[]::new);
            final var outputFieldNames = ((StructSchema) output).fields().stream().map(DataField::name).toArray(String[]::new);
            assertArrayEquals(inputFieldNames, outputFieldNames, "Input schema field names should match output schema field names");
        });
    }

    @Test
    void dataTest() {
        NotationTest.dataTest("csv", new CsvDataObjectMapper());
    }

    @Test
    void serdeTest() {
        NotationTest.serdeTest("csv", new CsvNotation("csv", new NativeDataObjectMapper()));
    }
}
