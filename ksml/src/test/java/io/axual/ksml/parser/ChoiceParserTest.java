package io.axual.ksml.parser;

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
import io.axual.ksml.exception.ParseException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.axual.ksml.parser.ParseTestSupport.nodeOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the parser behind every {@code type:} discriminator in a KSML definition.
 *
 * <p>The container cases exist because Jackson 3 changed what happens when a node is rendered as a
 * string: {@code asText()} used to return an empty string for an object or array, while
 * {@code asString()} throws. Without a kind check the user would see a raw Jackson coercion message
 * instead of a KSML parse error naming the file, the line and the accepted values.</p>
 */
class ChoiceParserTest {

    private final ChoiceParser<String> parser = newParser();

    private static ChoiceParser<String> newParser() {
        return new ChoiceParser<>(
                "type",
                "StoreType",
                "state store",
                "keyValue",
                Map.of(
                        "keyValue", StructsParser.of(node -> "parsed:keyValue", schema("KeyValueStore")),
                        "session", StructsParser.of(node -> "parsed:session", schema("SessionStore")),
                        "window", StructsParser.of(node -> "parsed:window", schema("WindowStore"))));
    }

    private static StructSchema schema(String name) {
        return new StructSchema("io.axual.ksml.test", name, "test schema", List.of());
    }

    @Test
    @DisplayName("A known type is routed to the matching parser")
    void parsesKnownType() throws Exception {
        assertThat(parser.parse(nodeOf("type: session"))).isEqualTo("parsed:session");
    }

    @Test
    @DisplayName("A missing type falls back to the default")
    void missingTypeUsesDefault() throws Exception {
        assertThat(parser.parse(nodeOf("someOtherField: value"))).isEqualTo("parsed:keyValue");
    }

    @Test
    @DisplayName("An unknown string type reports the accepted values")
    void unknownStringTypeReportsChoices() throws Exception {
        final var node = nodeOf("type: notAStoreType");

        assertThatThrownBy(() -> parser.parse(node))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unknown state store \"type\"")
                .hasMessageContaining("choose one of keyValue, session, window");
    }

    @Test
    @DisplayName("An object as the type value gives a parse error, not a Jackson coercion error")
    void objectTypeReportsParseError() throws Exception {
        final var node = nodeOf("""
                type:
                  oops: keyValue
                """);

        assertThatThrownBy(() -> parser.parse(node))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Expected a string for \"type\"")
                .hasMessageContaining("OBJECT")
                .hasMessageContaining("choose one of keyValue, session, window")
                .hasMessageNotContaining("cannot coerce");
    }

    @Test
    @DisplayName("An array as the type value gives a parse error, not a Jackson coercion error")
    void arrayTypeReportsParseError() throws Exception {
        final var node = nodeOf("""
                type:
                  - keyValue
                  - session
                """);

        assertThatThrownBy(() -> parser.parse(node))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Expected a string for \"type\"")
                .hasMessageContaining("ARRAY")
                .hasMessageContaining("choose one of keyValue, session, window")
                .hasMessageNotContaining("cannot coerce");
    }

    @Test
    @DisplayName("A non-string scalar is still coerced, as it was before Jackson 3")
    void numberTypeIsCoercedToString() throws Exception {
        // 1.x turned this into "5" and then failed on the unknown value. Keep that, so only containers
        // take the new path.
        final var node = nodeOf("type: 5");

        assertThatThrownBy(() -> parser.parse(node))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unknown state store \"type\"");
    }
}
