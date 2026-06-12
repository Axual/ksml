package io.axual.ksml.parser;

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

import tools.jackson.databind.JsonNode;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.YAMLObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Tests that integerField and longField reject YAML literals that would silently overflow.
class DefinitionParserFieldValidationTest {

    // Exposes the protected integerField/longField helpers for direct unit testing.
    // Using StructsParser.parse() directly avoids the FatalError wrapping in DefinitionParser.parse().
    private static class FieldTestParser extends DefinitionParser<Void> {
        final StructsParser<Integer> intField = integerField("value", "test integer field");
        final StructsParser<Long> longField = longField("value", "test long field");

        @Override
        public StructsParser<Void> parser() {
            return null; // not used directly
        }
    }

    private static final FieldTestParser PARSER = new FieldTestParser();

    private ParseNode nodeOf(String yaml) throws Exception {
        final var root = YAMLObjectMapper.INSTANCE.readValue(yaml, JsonNode.class);
        return ParseNode.fromRoot(root, "test");
    }

    // --- integerField ---

    @Test
    void integerField_throwsForValueExceedingIntRange() throws Exception {
        // 9999999999 fits in a long but overflows int; the old code silently truncated to 1410065407
        final var node = nodeOf("value: 9999999999");
        final var ex = assertThrows(ParseException.class, () -> PARSER.intField.parse(node));
        assertThat(ex.getMessage(), containsString("'value' is out of INT range"));
    }

    @Test
    void integerField_throwsForNegativeValueExceedingIntRange() throws Exception {
        final var node = nodeOf("value: -9999999999");
        final var ex = assertThrows(ParseException.class, () -> PARSER.intField.parse(node));
        assertThat(ex.getMessage(), containsString("'value' is out of INT range"));
    }

    @Test
    void integerField_acceptsValidPositiveIntValue() throws Exception {
        final var node = nodeOf("value: 4");
        assertDoesNotThrow(() -> PARSER.intField.parse(node));
    }

    @Test
    void integerField_acceptsMaxIntValue() throws Exception {
        final var node = nodeOf("value: 2147483647");
        assertDoesNotThrow(() -> PARSER.intField.parse(node));
    }

    // --- longField ---

    @Test
    void longField_throwsForFloatingPointValue() throws Exception {
        // 1.5 is not integral; the old code silently called longValue() which returned 1
        final var node = nodeOf("value: 1.5");
        final var ex = assertThrows(ParseException.class, () -> PARSER.longField.parse(node));
        assertThat(ex.getMessage(), containsString("'value' is not a valid long integer"));
    }

    @Test
    void longField_throwsForValueExceedingLongRange() throws Exception {
        // 99999999999999999999 exceeds Long.MAX_VALUE; Jackson parses it as BigInteger
        final var node = nodeOf("value: 99999999999999999999");
        final var ex = assertThrows(ParseException.class, () -> PARSER.longField.parse(node));
        assertThat(ex.getMessage(), containsString("'value' is not a valid long integer"));
    }

    @Test
    void longField_acceptsSmallIntegerValue() throws Exception {
        final var node = nodeOf("value: 5");
        assertDoesNotThrow(() -> PARSER.longField.parse(node));
    }

    @Test
    void longField_acceptsLargeValueWithinLongRange() throws Exception {
        // 5000000000 exceeds Integer.MAX_VALUE but is well within Long.MAX_VALUE
        final var node = nodeOf("value: 5000000000");
        assertDoesNotThrow(() -> PARSER.longField.parse(node));
    }
}
