package io.axual.ksml.runner.config.internal;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Represents a KSML definition that can be supplied in two different ways in user configuration:
 * - as a String that points to a file on disk (see {@link KsmlFilePath})
 * - as an inline JSON object embedded directly in the configuration (see {@link KsmlInlineDefinition}).
 *
 * This sealed interface is used both for Jackson (de-)serialization and for JSON Schema generation.
 *
 * JsonSubTypes names deliberately match JSON value kinds:
 * - "string" maps to {@link KsmlFilePath}
 * - "object" maps to {@link KsmlInlineDefinition}
 */
@JsonDeserialize(using = KsmlFileOrDefinitionDeserializer.class)
@JsonSerialize(using = KsmlFileOrDefinitionSerializer.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = KsmlFilePath.class, name = "string"),
        @JsonSubTypes.Type(value = KsmlInlineDefinition.class, name = "object")
})
public sealed interface KsmlFileOrDefinition permits KsmlFilePath, KsmlInlineDefinition {
    /**
     * Returns the raw value represented by this instance:
     * - a String for {@link KsmlFilePath}
     * - an ObjectNode for {@link KsmlInlineDefinition}
     */
    Object getValue();

}
