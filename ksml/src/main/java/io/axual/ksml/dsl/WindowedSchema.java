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
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.WindowedType;

import java.util.ArrayList;
import java.util.function.Function;

public class WindowedSchema {
    private WindowedSchema() {
    }

    // Public constants are the field names
    public static final String WINDOWED_SCHEMA_START_FIELD = "start";
    public static final String WINDOWED_SCHEMA_END_FIELD = "end";
    public static final String WINDOWED_SCHEMA_START_TIME_FIELD = "startTime";
    public static final String WINDOWED_SCHEMA_END_TIME_FIELD = "endTime";
    public static final String WINDOWED_SCHEMA_KEY_FIELD = "key";

    // Private constants are the schema descriptions and doc fields
    public static final String WINDOWED_SCHEMA_DOC_PREFIX = "Windowed ";
    private static final String WINDOWED_SCHEMA_START_FIELD_DOC = "Start timestamp in milliseconds";
    private static final String WINDOWED_SCHEMA_END_FIELD_DOC = "End timestamp in milliseconds";
    private static final String WINDOWED_SCHEMA_START_TIME_FIELD_DOC = "Start time in UTC";
    private static final String WINDOWED_SCHEMA_END_TIME_FIELD_DOC = "End time in UTC";
    private static final String WINDOWED_SCHEMA_KEY_FIELD_DOC = "Window key";

    public static StructSchema generateWindowedSchema(WindowedType windowedType, Function<DataType, DataSchema> dataTypeToSchema) {
        var fields = new ArrayList<DataField>();
        fields.add(new DataField(WINDOWED_SCHEMA_START_FIELD, DataSchema.LONG_SCHEMA, WINDOWED_SCHEMA_START_FIELD_DOC));
        fields.add(new DataField(WINDOWED_SCHEMA_END_FIELD, DataSchema.LONG_SCHEMA, WINDOWED_SCHEMA_END_FIELD_DOC));
        fields.add(new DataField(WINDOWED_SCHEMA_START_TIME_FIELD, DataSchema.STRING_SCHEMA, WINDOWED_SCHEMA_START_TIME_FIELD_DOC));
        fields.add(new DataField(WINDOWED_SCHEMA_END_TIME_FIELD, DataSchema.STRING_SCHEMA, WINDOWED_SCHEMA_END_TIME_FIELD_DOC));

        var keySchema = dataTypeToSchema.apply(windowedType.keyType());
        fields.add(new DataField(WINDOWED_SCHEMA_KEY_FIELD, keySchema, WINDOWED_SCHEMA_KEY_FIELD_DOC));

        return new StructSchema(
                DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE,
                schemaName(windowedType),
                WINDOWED_SCHEMA_DOC_PREFIX + windowedType.keyType().name(),
                fields,
                false);
    }

    private static String schemaName(WindowedType windowedType) {
        final var keyType = windowedType.keyType().name();
        final var type = keyType != null && !keyType.isEmpty() ? keyType : "type";
        return "Windowed" + type.substring(0, 1).toUpperCase() + type.substring(1);
    }
}
