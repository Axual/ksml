package io.axual.ksml.runner.config;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.axual.ksml.runner.config.ErrorHandlingConfig.ErrorTypeHandlingConfig;
import io.axual.ksml.runner.config.ErrorHandlingConfig.ErrorTypeHandlingConfig.Handler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ErrorHandlingHandlerTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();

    @ParameterizedTest(name = "forValue(\"{0}\") -> {1}")
    @CsvSource({
            "stopOnFail,     STOP",
            "continueOnFail, CONTINUE",
            "retryOnFail,    RETRY"
    })
    void forValueResolvesEveryCanonicalWireName(String input, Handler expected) {
        assertThat(Handler.forValue(input)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "forValue(\"{0}\") throws")
    @ValueSource(strings = {
            "stop",        // legacy short form is no longer accepted (only the canonical wire name is)
            "continue",
            "retry",
            "stpOnFail",   // typo
            "STOP",        // wrong case
            "stopOnFail ", // trailing whitespace
            ""             // empty
    })
    void forValueThrowsForUnknownInput(String input) {
        assertThatThrownBy(() -> Handler.forValue(input))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown error handler: " + input)
                .hasMessageContaining("Valid values:");
    }

    @Test
    void forValueReturnsNullForNullInput() {
        assertThat(Handler.forValue(null)).isNull();
    }

    @Test
    void jsonValueSerializesToCanonicalWireName() {
        assertThat(Handler.STOP.jsonValue()).isEqualTo("stopOnFail");
        assertThat(Handler.CONTINUE.jsonValue()).isEqualTo("continueOnFail");
        assertThat(Handler.RETRY.jsonValue()).isEqualTo("retryOnFail");
    }

    @ParameterizedTest(name = "{0} serializes to \"{1}\"")
    @CsvSource({
            "STOP,     stopOnFail",
            "CONTINUE, continueOnFail",
            "RETRY,    retryOnFail"
    })
    void serializesToCanonicalWireNameNotAlias(Handler handler, String wireName) throws Exception {
        // Jackson must serialize via @JsonValue (the canonical long form), never the short alias.
        assertThat(JSON.writeValueAsString(handler)).isEqualTo("\"" + wireName + "\"");
    }

    @Test
    void canonicalWireNameRoundTripsToSameConstant() {
        for (final var h : Handler.values()) {
            assertThat(Handler.forValue(h.jsonValue()))
                    .as("round-trip for %s", h)
                    .isEqualTo(h);
        }
    }

    @Test
    void deserializesHandlerFromYamlUsingCanonicalName() throws Exception {
        final var cfg = YAML.readValue("handler: continueOnFail\n", ErrorTypeHandlingConfig.class);
        assertThat(cfg.handler()).isEqualTo(Handler.CONTINUE);
    }

    @Test
    void legacyShortFormFailsDeserialization() {
        // The short "retry" form was historically accepted but is not advertised by the schema,
        // so it is now rejected to keep the config and the ksml-runner-spec in sync.
        final var thrown = assertThrows(Exception.class,
                () -> YAML.readValue("handler: retry\n", ErrorTypeHandlingConfig.class));
        assertThat(thrown).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(thrown.getMessage()).contains("Unknown error handler: retry");
    }

    @Test
    void unknownHandlerFailsDeserialization() {
        final var thrown = assertThrows(Exception.class,
                () -> YAML.readValue("handler: stpOnFail\n", ErrorTypeHandlingConfig.class));
        assertThat(thrown).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(thrown.getMessage()).contains("Unknown error handler: stpOnFail");
    }
}
