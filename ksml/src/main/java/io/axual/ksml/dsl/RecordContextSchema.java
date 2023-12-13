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

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;

public class RecordContextSchema {
    private RecordContextSchema() {
    }

    // Public constants are the fixed schemas and the field names
    public static final StructSchema RECORD_CONTEXT_HEADER_SCHEMA = generateRecordContextHeaderSchema();
    public static final StructSchema RECORD_CONTEXT_SCHEMA = generateRecordContextSchema();
    public static final String RECORD_CONTEXT_SCHEMA_NAME = "RecordContext";
    public static final String RECORD_CONTEXT_SCHEMA_HEADERS_FIELD = "headers";
    public static final String RECORD_CONTEXT_SCHEMA_OFFSET_FIELD = "offset";
    public static final String RECORD_CONTEXT_SCHEMA_PARTITION_FIELD = "partition";
    public static final String RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD = "timestamp";
    public static final String RECORD_CONTEXT_SCHEMA_TOPIC_FIELD = "topic";
    public static final String RECORD_CONTEXT_HEADER_SCHEMA_NAME = "Header";
    public static final String RECORD_CONTEXT_HEADER_SCHEMA_KEY_FIELD = "key";
    public static final String RECORD_CONTEXT_HEADER_SCHEMA_VALUE_FIELD = "value";

    // Private constants are the schema descriptions and doc fields
    private static final String RECORD_CONTEXT_SCHEMA_OFFSET_DOC = "Message offset";
    private static final String RECORD_CONTEXT_SCHEMA_TIMESTAMP_DOC = "Message timestamp";
    private static final String RECORD_CONTEXT_SCHEMA_TOPIC_DOC = "Message topic";
    private static final String RECORD_CONTEXT_SCHEMA_PARTITION_DOC = "Message partition";
    private static final String RECORD_CONTEXT_SCHEMA_HEADERS_DOC = "Message headers";
    private static final String RECORD_CONTEXT_HEADER_SCHEMA_KEY_DOC = "Header key";
    private static final String RECORD_CONTEXT_HEADER_SCHEMA_VALUE_DOC = "Header value";
    private static final String KAFKA_PREFIX = "Kafka ";

    private static StructSchema generateRecordContextSchema() {
        final var fields = new ArrayList<DataField>();
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_OFFSET_FIELD, DataSchema.create(DataSchema.Type.LONG), RECORD_CONTEXT_SCHEMA_OFFSET_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD, DataSchema.create(DataSchema.Type.LONG), RECORD_CONTEXT_SCHEMA_TIMESTAMP_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_TOPIC_FIELD, DataSchema.create(DataSchema.Type.STRING), RECORD_CONTEXT_SCHEMA_TOPIC_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_PARTITION_FIELD, DataSchema.create(DataSchema.Type.INTEGER), RECORD_CONTEXT_SCHEMA_PARTITION_DOC));
        fields.add(new DataField(RECORD_CONTEXT_SCHEMA_HEADERS_FIELD, new ListSchema(RECORD_CONTEXT_HEADER_SCHEMA), RECORD_CONTEXT_SCHEMA_HEADERS_DOC));
        return new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, RECORD_CONTEXT_SCHEMA_NAME, KAFKA_PREFIX + RECORD_CONTEXT_SCHEMA_NAME, fields);
    }

    private static StructSchema generateRecordContextHeaderSchema() {
        final var headerFields = new ArrayList<DataField>();
        headerFields.add(new DataField(RECORD_CONTEXT_HEADER_SCHEMA_KEY_FIELD, DataSchema.create(DataSchema.Type.STRING), RECORD_CONTEXT_HEADER_SCHEMA_KEY_DOC));
        headerFields.add(new DataField(RECORD_CONTEXT_HEADER_SCHEMA_VALUE_FIELD, DataSchema.create(DataSchema.Type.BYTES), RECORD_CONTEXT_HEADER_SCHEMA_VALUE_DOC));
        return new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, RECORD_CONTEXT_HEADER_SCHEMA_NAME, KAFKA_PREFIX + RECORD_CONTEXT_HEADER_SCHEMA_NAME, headerFields);
    }
}
