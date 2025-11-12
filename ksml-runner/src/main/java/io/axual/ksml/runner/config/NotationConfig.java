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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
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
        String type,
        @JsonProperty(value = "schemaRegistry", required = false)
        @JsonPropertyDescription("The name of the Schema Registry configuration to use, if applicable for the type")
        String schemaRegistry,
        @JsonProperty(value = "config", required = false)
        @JsonPropertyDescription("Additional properties for the serializer, these will be added to any configuration options defined for the the schema registry")
        StringMap config) {
}
