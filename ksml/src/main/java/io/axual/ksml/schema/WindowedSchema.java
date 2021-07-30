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

import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLExecutionException;

public class WindowedSchema {
    private static final String WINDOW_SCHEMA_DOC_PREFIX = "Windowed ";
    private static final String WINDOW_SCHEMA_NAMESPACE = "io.axual.ksml.data";
    public static final String START_FIELD = "start";
    private static final String START_FIELD_DOC = "Start timestamp";
    public static final String END_FIELD = "end";
    private static final String END_FIELD_DOC = "End timestamp";
    public static final String START_TIME_FIELD = "startTime";
    private static final String START_TIME_FIELD_DOC = "Start time";
    public static final String END_TIME_FIELD = "endTime";
    private static final String END_TIME_FIELD_DOC = "End time";
    public static final String KEY_FIELD = "key";
    private static final String KEY_FIELD_DOC = "Window key";

    private WindowedSchema() {
    }

    public static Schema generateWindowedSchema(WindowedType windowedType) {
        var schemaName = windowedType.schemaName();
        var builder = new SchemaBuilder(schemaName, WINDOW_SCHEMA_DOC_PREFIX + windowedType.keyType().schemaName(), WINDOW_SCHEMA_NAMESPACE);
        builder.addField(START_FIELD, Schema.create(Schema.Type.LONG), START_FIELD_DOC, null);
        builder.addField(END_FIELD, Schema.create(Schema.Type.LONG), END_FIELD_DOC, null);
        builder.addField(START_TIME_FIELD, Schema.create(Schema.Type.STRING), START_TIME_FIELD_DOC, null);
        builder.addField(END_TIME_FIELD, Schema.create(Schema.Type.STRING), END_TIME_FIELD_DOC, null);

        var keySchema = SchemaUtil.dataTypeToSchema(windowedType.keyType());
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
