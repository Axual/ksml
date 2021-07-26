package io.axual.ksml.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import org.apache.avro.Schema;

import io.axual.ksml.data.type.WindowType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.util.SchemaUtil;

public class WindowSchema {
    private static final String WINDOW_SCHEMA_DOC = "Window record";
    private static final String WINDOW_SCHEMA_NAMESPACE = "io.axual.ksml.data";
    public static final String START_FIELD = "start";
    private static final String START_FIELD_DOC = "Start timestamp";
    public static final String END_FIELD = "end";
    private static final String END_FIELD_DOC = "End timestamp";
    public static final String START_TIME_FIELD = "startTime";
    private static final String START_TIME_FIELD_DOC = "Start time";
    public static final String END_TIME_FIELD = "endTime";
    private static final String END_TIME_FIELD_DOC = "End time";
    private static final String DEFAULT_TIME_VALUE = "1970-01-01T00:00:00Z";
    public static final String KEY_FIELD = "key";
    private static final String KEY_FIELD_DOC = "Windowed key";

    private WindowSchema() {
    }

    public static DataSchema generateWindowSchema(DataType keyType) {
        var schemaName = new WindowType(keyType).schemaName();
        var builder = DataSchema.newBuilder(schemaName, WINDOW_SCHEMA_DOC, WINDOW_SCHEMA_NAMESPACE);
        builder.addField(START_FIELD, Schema.create(Schema.Type.LONG), START_FIELD_DOC, 0);
        builder.addField(END_FIELD, Schema.create(Schema.Type.LONG), END_FIELD_DOC, 0);
        builder.addField(START_TIME_FIELD, Schema.create(Schema.Type.STRING), START_TIME_FIELD_DOC, DEFAULT_TIME_VALUE);
        builder.addField(END_TIME_FIELD, Schema.create(Schema.Type.STRING), END_TIME_FIELD_DOC, DEFAULT_TIME_VALUE);

        var keySchema = SchemaUtil.dataTypeToSchema(keyType);
        switch (keySchema.getType()) {
            case BOOLEAN:
                builder.addField(KEY_FIELD, keySchema, KEY_FIELD_DOC, false);
                break;
            case INT:
                builder.addField(KEY_FIELD, keySchema, KEY_FIELD_DOC, (Integer) 0);
                break;
            case LONG:
                builder.addField(KEY_FIELD, keySchema, KEY_FIELD_DOC, 0L);
                break;
            case FLOAT:
                builder.addField(KEY_FIELD, keySchema, KEY_FIELD_DOC, 0.0f);
                break;
            case DOUBLE:
                builder.addField(KEY_FIELD, keySchema, KEY_FIELD_DOC, 0.0d);
                break;
            case BYTES:
            case STRING:
            case ARRAY:
            case RECORD:
                builder.addField(KEY_FIELD, keySchema, KEY_FIELD_DOC, null);
                break;
            default:
                throw new KSMLExecutionException("Can not add field for type " + keySchema.getType());
        }
        return builder.build();
    }
}
