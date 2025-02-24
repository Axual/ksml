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

import io.axual.ksml.data.schema.*;

import java.util.ArrayList;

import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA;

public class RecordContextSchema {
    private RecordContextSchema() {
    }

    // Public constants are the fixed schemas and the field names
    public static final StructSchema RECORD_CONTEXT_SCHEMA = generateRecordContextSchema();
    public static final String RECORD_CONTEXT_SCHEMA_NAME = "RecordContext";
    public static final String RECORD_CONTEXT_SCHEMA_HEADERS_FIELD = "headers";
    public static final String RECORD_CONTEXT_SCHEMA_OFFSET_FIELD = "offset";
    public static final String RECORD_CONTEXT_SCHEMA_PARTITION_FIELD = "partition";
    public static final String RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD = "timestamp";
    public static final String RECORD_CONTEXT_SCHEMA_TOPIC_FIELD = "topic";

    // Private constants are the schema descriptions and doc fields
    private static final String RECORD_CONTEXT_SCHEMA_OFFSET_DOC = "Message offset";
    private static final String RECORD_CONTEXT_SCHEMA_TIMESTAMP_DOC = "Message timestamp";
    private static final String RECORD_CONTEXT_SCHEMA_TOPIC_DOC = "Message topic";
    private static final String RECORD_CONTEXT_SCHEMA_PARTITION_DOC = "Message partition";
    private static final String RECORD_CONTEXT_SCHEMA_HEADERS_DOC = "Message headers";
    private static final String KAFKA_PREFIX = "Kafka ";

    private static StructSchema generateRecordContextSchema() {
        final var fields = new ArrayList<DataField>();
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_OFFSET_FIELD, DataSchema.LONG_SCHEMA, RECORD_CONTEXT_SCHEMA_OFFSET_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD, DataSchema.LONG_SCHEMA, RECORD_CONTEXT_SCHEMA_TIMESTAMP_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_TOPIC_FIELD, DataSchema.STRING_SCHEMA, RECORD_CONTEXT_SCHEMA_TOPIC_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_PARTITION_FIELD, DataSchema.INTEGER_SCHEMA, RECORD_CONTEXT_SCHEMA_PARTITION_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_HEADERS_FIELD, new ListSchema(HEADER_SCHEMA), RECORD_CONTEXT_SCHEMA_HEADERS_DOC));
        return new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, RECORD_CONTEXT_SCHEMA_NAME, KAFKA_PREFIX + RECORD_CONTEXT_SCHEMA_NAME, fields);
    }
}
