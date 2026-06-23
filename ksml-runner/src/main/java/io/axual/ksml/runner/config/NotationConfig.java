package io.axual.ksml.runner.config;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonValue;
import io.axual.ksml.runner.config.internal.StringMap;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonClassDescription("Specify and configure a specific type of notation, or format on how to read/write data from a Kafka topic.")
@Builder
@Jacksonized
public record NotationConfig(
        @JsonProperty(value = "type", required = true)
        @JsonPropertyDescription("Serializer implementation type")
        NotationType type,
        @JsonProperty(value = "schemaRegistry", required = false)
        @JsonPropertyDescription("The name of the Schema Registry configuration to use, if applicable for the type")
        String schemaRegistry,
        @JsonProperty(value = "config", required = false)
        @JsonPropertyDescription("Additional properties for the serializer, these will be added to any configuration options defined for the the schema registry")
        StringMap config) {

    @JsonClassDescription("Supported notation serializer implementations.")
    public enum NotationType {
        // Schema-registry-backed notations (loaded via ServiceLoader)
        @JsonProperty("apicurio_avro")
        APICURIO_AVRO("apicurio_avro"),
        @JsonProperty("confluent_avro")
        CONFLUENT_AVRO("confluent_avro"),
        @JsonProperty("apicurio_jsonschema")
        APICURIO_JSONSCHEMA("apicurio_jsonschema"),
        @JsonProperty("apicurio_protobuf")
        APICURIO_PROTOBUF("apicurio_protobuf"),
        // Built-in notations (csv, xml are loaded via ServiceLoader; json, binary are hardcoded
        // in NotationFactories). The reference docs treat these as "no configuration needed",
        // but the runtime accepts them in the notations block, so the schema mirrors that.
        @JsonProperty("csv")
        CSV("csv"),
        @JsonProperty("xml")
        XML("xml"),
        @JsonProperty("json")
        JSON("json"),
        @JsonProperty("binary")
        BINARY("binary");
        // TODO: ConfluentJsonSchemaNotationProvider and ConfluentProtobufNotationProvider exist
        // in ksml-data-jsonschema-confluent and ksml-data-protobuf-confluent but their
        // META-INF/services files are empty, so they are not loaded via ServiceLoader. Once
        // those providers are registered, add "confluent_jsonschema" and "confluent_protobuf"
        // here so the schema accepts them.

        private final String jsonValue;

        NotationType(String jsonValue) {
            this.jsonValue = jsonValue;
        }

        @JsonValue
        public String jsonValue() {
            return jsonValue;
        }

        @JsonCreator
        public static NotationType forValue(String value) {
            if (value == null) {
                return null;
            }
            for (NotationType nt : values()) {
                if (nt.jsonValue.equals(value)) {
                    return nt;
                }
            }
            return null;
        }
    }
}
