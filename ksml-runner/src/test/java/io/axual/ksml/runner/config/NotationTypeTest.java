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
import io.axual.ksml.runner.config.NotationConfig.NotationType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NotationTypeTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper JSON = new ObjectMapper();

    @ParameterizedTest(name = "forValue(\"{0}\") -> {1}")
    @CsvSource({
            "apicurio_avro,      APICURIO_AVRO",
            "confluent_avro,     CONFLUENT_AVRO",
            "apicurio_jsonschema,APICURIO_JSONSCHEMA",
            "apicurio_protobuf,  APICURIO_PROTOBUF",
            "csv,                CSV",
            "xml,                XML",
            "json,               JSON",
            "binary,             BINARY"
    })
    void forValueResolvesEveryRegisteredWireName(String wireName, NotationType expected) {
        assertThat(NotationType.forValue(wireName)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "forValue(\"{0}\") throws")
    @ValueSource(strings = {
            "confluentavro",          // typo from issue #369 demo
            "apicurio_json_schema",   // typo from issue #369 description
            "AVRO",                   // wrong case
            "apicurio_avro ",         // trailing whitespace
            ""                        // empty
    })
    void forValueThrowsForUnknownInput(String input) {
        assertThatThrownBy(() -> NotationType.forValue(input))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown notation type: " + input)
                .hasMessageContaining("Valid values:");
    }

    @Test
    void forValueReturnsNullForNullInput() {
        assertThat(NotationType.forValue(null)).isNull();
    }

    @ParameterizedTest(name = "{1} serializes to \"{0}\"")
    @CsvSource({
            "apicurio_avro,      APICURIO_AVRO",
            "confluent_avro,     CONFLUENT_AVRO",
            "apicurio_jsonschema,APICURIO_JSONSCHEMA",
            "apicurio_protobuf,  APICURIO_PROTOBUF",
            "csv,                CSV",
            "xml,                XML",
            "json,               JSON",
            "binary,             BINARY"
    })
    void serializesEachConstantToWireName(String wireName, NotationType nt) throws Exception {
        assertThat(JSON.writeValueAsString(nt)).isEqualTo("\"" + wireName + "\"");
    }

    @Test
    void jsonValueRoundTripsToSameConstant() {
        for (final var nt : NotationType.values()) {
            assertThat(NotationType.forValue(nt.jsonValue()))
                    .as("round-trip for %s", nt)
                    .isEqualTo(nt);
        }
    }

    @Test
    void deserializesValidNotationConfigFromYaml() throws Exception {
        final var yaml = """
                type: confluent_avro
                schemaRegistry: my_sr
                """;
        final var cfg = YAML.readValue(yaml, NotationConfig.class);
        assertThat(cfg.type()).isEqualTo(NotationType.CONFLUENT_AVRO);
        assertThat(cfg.schemaRegistry()).isEqualTo("my_sr");
    }

    @Test
    void unknownPropertyFailsDeserialization() {
        // NotationConfig rejects unknown properties (ignoreUnknown = false), matching the
        // schema's "additionalProperties": false, so a typo'd key fails fast instead of being
        // silently dropped.
        final var yaml = """
                type: confluent_avro
                schemaRegsitry: my_sr
                """;
        assertThrows(Exception.class, () -> YAML.readValue(yaml, NotationConfig.class));
    }

    @Test
    void unknownTypeFailsDeserialization() {
        final var yaml = """
                type: confluentavro
                """;
        // @JsonCreator forValue throws for unknown values -> Jackson wraps it and fails the
        // deserialization, so the runner stops at startup with a clear error instead of
        // silently registering a broken notation.
        final var thrown = assertThrows(Exception.class,
                () -> YAML.readValue(yaml, NotationConfig.class));
        assertThat(thrown).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(thrown.getMessage()).contains("Unknown notation type: confluentavro");
    }
}
