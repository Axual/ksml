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

import io.axual.ksml.exception.ParseException;
import io.axual.ksml.parser.ParseTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An enum symbol is a plain name. A container is not one, and Jackson 3 refuses to render it as a
 * string, so the parser reports a parse error rather than producing an empty symbol name.
 */
class LegacySymbolParserTest {

    private final LegacySymbolParser parser = new LegacySymbolParser();

    private String parseSymbol(String yaml) throws Exception {
        return parser.parse(ParseTestSupport.nodeOf(yaml).get("symbol"));
    }

    @Test
    @DisplayName("A string symbol is returned as-is")
    void parsesStringSymbol() throws Exception {
        assertThat(parseSymbol("symbol: TEMPERATURE")).isEqualTo("TEMPERATURE");
    }

    @Test
    @DisplayName("An object symbol reports a parse error, not a Jackson coercion error")
    void objectSymbolReportsParseError() {
        final var yaml = """
                symbol:
                  name: TEMPERATURE
                """;

        assertThatThrownBy(() -> parseSymbol(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Could not parse enum symbol")
                .hasMessageNotContaining("cannot coerce");
    }

    @Test
    @DisplayName("An array symbol reports a parse error, not a Jackson coercion error")
    void arraySymbolReportsParseError() {
        final var yaml = """
                symbol:
                  - TEMPERATURE
                  - HUMIDITY
                """;

        assertThatThrownBy(() -> parseSymbol(yaml))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Could not parse enum symbol")
                .hasMessageNotContaining("cannot coerce");
    }

    @Test
    @DisplayName("A null node reports a parse error")
    void nullNodeReportsParseError() {
        assertThatThrownBy(() -> parser.parse(null))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Could not parse enum symbol");
    }
}
