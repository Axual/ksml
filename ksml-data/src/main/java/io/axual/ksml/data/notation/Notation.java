package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Serde;

/**
 * Describes a data notation used by KSML for serializing/deserializing records and parsing schemas.
 * Implementations provide the default data type, serde creation, and conversion/parsing capabilities.
 */
public interface Notation {
    enum SchemaUsage {
        SCHEMALESS_ONLY,    // Notation only supports schemaless
        SCHEMA_OPTIONAL,    // Notation supports schemaless and schema
        SCHEMA_REQUIRED     // Notation only works with an explicit schema
    }

    /**
     * Indicates the dependence of the notation on a schema. When SCHEMALESS_ONLY, only user types of the form
     * "notation_name" are accepted. When SCHEMA_REQUIRED, only "notation_name:SchemaName" is accepted. In the case of
     * SCHEMA_OPTIONAL, both are accepted.
     *
     * @return the schema usage indicator
     */
    SchemaUsage schemaUsage();

    /**
     * Returns the default data type handled by this notation. This is typically the most
     * natural in-memory representation for data encoded with the notation.
     *
     * @return the default DataType for this notation
     */
    DataType defaultType();

    /**
     * Returns the fully qualified name of this notation, potentially including the vendor prefix
     * when applicable (e.g. "vendor_notation").
     *
     * @return the notation name
     */
    String name();

    /**
     * Returns the default filename extension used for artifacts belonging to this notation
     * (for example, ".avsc", ".proto", ".json").
     *
     * @return the filename extension, or null when none
     */
    String filenameExtension();

    /**
     * Creates a Kafka Serde for the given data type and key/value role.
     *
     * @param type  the data type to serialize/deserialize
     * @param isKey whether the serde will be used for keys (true) or values (false)
     * @return a configured Serde instance
     * @throws RuntimeException when the type is not supported by this notation
     */
    Serde<Object> serde(DataType type, boolean isKey);

    /**
     * A converter that can transform a DataObject into another representation according to a target type.
     */
    interface Converter {
        /**
         * Converts a value into the requested target data type.
         *
         * @param value      the source value
         * @param targetType the desired DataType
         * @return the converted value
         * @throws RuntimeException when the conversion cannot be performed
         */
        DataObject convert(DataObject value, DataType targetType);
    }

    /**
     * Returns the converter used by this notation to transform data between types.
     *
     * @return the Converter instance
     */
    Converter converter();

    /**
     * Responsible for parsing textual schema definitions into DataSchema objects.
     */
    interface SchemaParser {
        /**
         * Parses a schema string within a given context and schema name.
         *
         * @param contextName  a human-readable context or namespace
         * @param schemaName   the logical name of the schema
         * @param schemaString the textual schema definition
         * @return a parsed DataSchema
         * @throws RuntimeException when the schema cannot be parsed
         */
        DataSchema parse(String contextName, String schemaName, String schemaString);
    }

    /**
     * Returns the schema parser used by this notation.
     *
     * @return the SchemaParser instance
     */
    SchemaParser schemaParser();

    /**
     * Returns whether this notation supports fetching schemas from a remote schema registry.
     * <p>
     * The default implementation returns {@code false}. Notations backed by a schema registry
     * (e.g., Confluent Avro) should override this to return {@code true}.
     *
     * @return {@code true} if {@link #fetchRemoteSchema(String)} is supported
     */
    default boolean supportsRemoteSchema() {
        return false;
    }

    /**
     * Fetches a schema from a remote schema registry by subject name.
     * <p>
     * The default implementation returns {@code null}, indicating that this notation does not
     * support remote schema fetching. Notations backed by a schema registry (e.g., Confluent Avro)
     * should override this method to query the registry for the latest version of the given subject.
     *
     * @param subject the schema registry subject name (e.g., "my-topic-value" or "my-topic-key")
     * @return the fetched DataSchema, or {@code null} if remote fetching is not supported
     */
    default DataSchema fetchRemoteSchema(String subject) {
        return null;
    }
}
