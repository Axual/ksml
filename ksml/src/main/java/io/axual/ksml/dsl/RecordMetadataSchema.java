package io.axual.ksml.dsl;

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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;

import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA;

public class RecordMetadataSchema {
    private RecordMetadataSchema() {
    }

    // Public constants are the fixed schemas and the field names
    public static final StructSchema RECORD_METADATA_SCHEMA = generateRecordMetadataSchema();
    public static final String RECORD_METADATA_SCHEMA_NAME = "RecordMetadata";
    public static final String RECORD_METADATA_SCHEMA_HEADERS_FIELD = "headers";
    public static final String RECORD_METADATA_SCHEMA_TIMESTAMP_FIELD = "timestamp";

    // Private constants are the schema descriptions and doc fields
    private static final String RECORD_METADATA_SCHEMA_TIMESTAMP_DOC = "Message timestamp";
    private static final String RECORD_METADATA_SCHEMA_HEADERS_DOC = "Message headers";
    private static final String KAFKA_PREFIX = "Kafka ";

    private static StructSchema generateRecordMetadataSchema() {
        final var fields = new ArrayList<StructSchema.Field>();
        fields.add(new StructSchema.Field(RECORD_METADATA_SCHEMA_TIMESTAMP_FIELD, DataSchema.LONG_SCHEMA, RECORD_METADATA_SCHEMA_TIMESTAMP_DOC, 1));
        fields.add(new StructSchema.Field(RECORD_METADATA_SCHEMA_HEADERS_FIELD, new ListSchema(HEADER_SCHEMA), RECORD_METADATA_SCHEMA_HEADERS_DOC, 2));
        return new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, RECORD_METADATA_SCHEMA_NAME, KAFKA_PREFIX + RECORD_METADATA_SCHEMA_NAME, fields, false);
    }
}
