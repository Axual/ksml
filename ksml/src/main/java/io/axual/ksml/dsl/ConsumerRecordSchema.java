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
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;

public class ConsumerRecordSchema {
    private ConsumerRecordSchema() {
    }

    // Public constants are the fixed schemas and the field names
    public static final StructSchema CONSUMER_RECORD_SCHEMA = generateConsumerRecordSchema();
    public static final String CONSUMER_RECORD_SCHEMA_NAME = "ConsumerRecord";
    public static final String CONSUMER_RECORD_SCHEMA_TIMESTAMP_FIELD = "timestamp";
    public static final String CONSUMER_RECORD_SCHEMA_TIMESTAMP_TYPE_FIELD = "timestampType";
    public static final String CONSUMER_RECORD_SCHEMA_KEY_FIELD = "key";
    public static final String CONSUMER_RECORD_SCHEMA_VALUE_FIELD = "value";
    public static final String CONSUMER_RECORD_SCHEMA_TOPIC_FIELD = "topic";
    public static final String CONSUMER_RECORD_SCHEMA_PARTITION_FIELD = "partition";
    public static final String CONSUMER_RECORD_SCHEMA_OFFSET_FIELD = "offset";

    // Private constants are the schema descriptions and doc fields
    private static final String CONSUMER_RECORD_SCHEMA_TIMESTAMP_DOC = "Record timestamp";
    private static final String CONSUMER_RECORD_SCHEMA_TIMESTAMP_TYPE_DOC = "Record timestamp type";
    public static final String CONSUMER_RECORD_SCHEMA_KEY_DOC = "Record key";
    public static final String CONSUMER_RECORD_SCHEMA_VALUE_DOC = "Record value";
    public static final String CONSUMER_RECORD_SCHEMA_TOPIC_DOC = "Record topic";
    public static final String CONSUMER_RECORD_SCHEMA_PARTITION_DOC = "Record partition";
    public static final String CONSUMER_RECORD_SCHEMA_OFFSET_DOC = "Record offset";
    private static final String KAFKA_PREFIX = "Kafka ";

    private static StructSchema generateConsumerRecordSchema() {
        final var fields = new ArrayList<StructSchema.Field>();
        fields.add(new StructSchema.Field(CONSUMER_RECORD_SCHEMA_TIMESTAMP_FIELD, DataSchema.LONG_SCHEMA, CONSUMER_RECORD_SCHEMA_TIMESTAMP_DOC, 1));
        fields.add(new StructSchema.Field(CONSUMER_RECORD_SCHEMA_TIMESTAMP_TYPE_FIELD, DataSchema.STRING_SCHEMA, CONSUMER_RECORD_SCHEMA_TIMESTAMP_TYPE_DOC, 2));
        fields.add(new StructSchema.Field(CONSUMER_RECORD_SCHEMA_KEY_FIELD, DataSchema.ANY_SCHEMA, CONSUMER_RECORD_SCHEMA_KEY_DOC, 3));
        fields.add(new StructSchema.Field(CONSUMER_RECORD_SCHEMA_VALUE_FIELD, DataSchema.ANY_SCHEMA, CONSUMER_RECORD_SCHEMA_VALUE_DOC, 4));
        fields.add(new StructSchema.Field(CONSUMER_RECORD_SCHEMA_TOPIC_FIELD, DataSchema.STRING_SCHEMA, CONSUMER_RECORD_SCHEMA_TOPIC_DOC, 5));
        fields.add(new StructSchema.Field(CONSUMER_RECORD_SCHEMA_PARTITION_FIELD, DataSchema.INTEGER_SCHEMA, CONSUMER_RECORD_SCHEMA_PARTITION_DOC, 6));
        fields.add(new StructSchema.Field(CONSUMER_RECORD_SCHEMA_OFFSET_FIELD, DataSchema.LONG_SCHEMA, CONSUMER_RECORD_SCHEMA_OFFSET_DOC, 7));
        return new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, CONSUMER_RECORD_SCHEMA_NAME, KAFKA_PREFIX + CONSUMER_RECORD_SCHEMA_NAME, fields, false);
    }
}
