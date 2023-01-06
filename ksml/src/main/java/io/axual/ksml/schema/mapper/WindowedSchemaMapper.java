package io.axual.ksml.schema.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaUtil;
import io.axual.ksml.schema.StructSchema;

public class WindowedSchemaMapper implements DataSchemaMapper<WindowedType> {
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

    @Override
    public StructSchema toDataSchema(WindowedType windowedType) {
        return new StructSchema(
                WINDOW_SCHEMA_NAMESPACE,
                windowedType.schemaName(),
                WINDOW_SCHEMA_DOC_PREFIX + windowedType.keyType().schemaName(),
                generateWindowKeySchema(windowedType));
    }

    @Override
    public WindowedType fromDataSchema(DataSchema object) {
        throw new KSMLExecutionException("Can not convert a DataSchema to a Windowed dataType");
    }

    private static List<DataField> generateWindowKeySchema(WindowedType windowedType) {
        var result = new ArrayList<DataField>();
        result.add(new DataField(START_FIELD, DataSchema.create(DataSchema.Type.LONG), START_FIELD_DOC, null, DataField.Order.ASCENDING));
        result.add(new DataField(END_FIELD, DataSchema.create(DataSchema.Type.LONG), END_FIELD_DOC, null, DataField.Order.ASCENDING));
        result.add(new DataField(START_TIME_FIELD, DataSchema.create(DataSchema.Type.STRING), START_TIME_FIELD_DOC, null, DataField.Order.ASCENDING));
        result.add(new DataField(END_TIME_FIELD, DataSchema.create(DataSchema.Type.STRING), END_TIME_FIELD_DOC, null, DataField.Order.ASCENDING));
        var keySchema = SchemaUtil.dataTypeToSchema(windowedType.keyType());
        result.add(new DataField(KEY_FIELD, keySchema, KEY_FIELD_DOC, null, DataField.Order.ASCENDING));
        return result;
    }
}
