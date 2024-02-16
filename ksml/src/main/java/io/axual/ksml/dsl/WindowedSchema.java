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
import io.axual.ksml.data.mapper.DataTypeSchemaMapper;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.WindowedType;

import java.util.ArrayList;
import java.util.List;

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
    private static final String WINDOWED_SCHEMA_START_FIELD_DOC = "Start timestamp";
    private static final String WINDOWED_SCHEMA_END_FIELD_DOC = "End timestamp";
    private static final String WINDOWED_SCHEMA_START_TIME_FIELD_DOC = "Start time";
    private static final String WINDOWED_SCHEMA_END_TIME_FIELD_DOC = "End time";
    private static final String WINDOWED_SCHEMA_KEY_FIELD_DOC = "Window key";
    private static final DataTypeSchemaMapper dataTypeSchemaMapper = new DataTypeSchemaMapper();

    public static StructSchema generateWindowedSchema(WindowedType windowedType) {
        return new StructSchema(
                DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE,
                windowedType.schemaName(),
                WINDOWED_SCHEMA_DOC_PREFIX + windowedType.keyType().schemaName(),
                generateWindowKeySchemaFields(windowedType));
    }

    private static List<DataField> generateWindowKeySchemaFields(WindowedType windowedType) {
        var result = new ArrayList<DataField>();
        result.add(new DataField(WINDOWED_SCHEMA_START_FIELD, DataSchema.create(DataSchema.Type.LONG), WINDOWED_SCHEMA_START_FIELD_DOC));
        result.add(new DataField(WINDOWED_SCHEMA_END_FIELD, DataSchema.create(DataSchema.Type.LONG), WINDOWED_SCHEMA_END_FIELD_DOC));
        result.add(new DataField(WINDOWED_SCHEMA_START_TIME_FIELD, DataSchema.create(DataSchema.Type.STRING), WINDOWED_SCHEMA_START_TIME_FIELD_DOC));
        result.add(new DataField(WINDOWED_SCHEMA_END_TIME_FIELD, DataSchema.create(DataSchema.Type.STRING), WINDOWED_SCHEMA_END_TIME_FIELD_DOC));
        var keySchema = dataTypeSchemaMapper.toDataSchema(windowedType.keyType());
        result.add(new DataField(WINDOWED_SCHEMA_KEY_FIELD, keySchema, WINDOWED_SCHEMA_KEY_FIELD_DOC));
        return result;
    }
}
