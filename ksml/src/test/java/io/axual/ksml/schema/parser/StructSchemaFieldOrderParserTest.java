package io.axual.ksml.schema.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.parser.ParseTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The order parser is deliberately lenient: anything it does not recognise becomes ASCENDING. These
 * tests pin that, including for objects and arrays, which Jackson 3 refuses to render as a string.
 */
class StructSchemaFieldOrderParserTest {

    private final StructSchemaFieldOrderParser parser = new StructSchemaFieldOrderParser();

    private StructSchema.Field.Order parseOrder(String yaml) throws Exception {
        return parser.parse(ParseTestSupport.nodeOf(yaml).get("order"));
    }

    @ParameterizedTest(name = "\"{0}\" is read as {1}")
    @CsvSource({
            "ascending,  ASCENDING",
            "descending, DESCENDING",
            "ignore,     IGNORE",
            "DESCENDING, DESCENDING"
    })
    @DisplayName("A known order name is read, whatever its case")
    void knownOrderNames(String value, StructSchema.Field.Order expected) throws Exception {
        assertThat(parseOrder("order: " + value)).isEqualTo(expected);
    }

    @Test
    @DisplayName("A missing order defaults to ASCENDING")
    void missingOrderDefaults() {
        assertThat(parser.parse(null)).isEqualTo(StructSchema.Field.Order.ASCENDING);
    }

    @Test
    @DisplayName("An unknown order name defaults to ASCENDING")
    void unknownOrderDefaults() throws Exception {
        assertThat(parseOrder("order: sideways")).isEqualTo(StructSchema.Field.Order.ASCENDING);
    }

    @Test
    @DisplayName("An object as the order value defaults to ASCENDING instead of throwing")
    void objectOrderDefaults() throws Exception {
        final var yaml = """
                order:
                  oops: ascending
                """;

        assertThat(parseOrder(yaml)).isEqualTo(StructSchema.Field.Order.ASCENDING);
    }

    @Test
    @DisplayName("An array as the order value defaults to ASCENDING instead of throwing")
    void arrayOrderDefaults() throws Exception {
        final var yaml = """
                order:
                  - ascending
                  - descending
                """;

        assertThat(parseOrder(yaml)).isEqualTo(StructSchema.Field.Order.ASCENDING);
    }
}
