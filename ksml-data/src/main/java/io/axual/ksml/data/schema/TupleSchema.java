package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.type.TupleType;

import java.util.ArrayList;
import java.util.List;

public class TupleSchema extends StructSchema {
    public TupleSchema(TupleType type, DataTypeDataSchemaMapper mapper) {
        super(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, type.toString(), "Tuple with " + type.subTypeCount() + " fields", convertTupleTypeToFields(type, mapper), false);
    }

    private static List<DataField> convertTupleTypeToFields(TupleType type, DataTypeDataSchemaMapper mapper) {
        if (type.subTypeCount() == 0) {
            throw new SchemaException("TupleSchema requires at least one field: type=" + type);
        }
        final var result = new ArrayList<DataField>();
        for (int index = 0; index < type.subTypeCount(); index++) {
            final var field = new DataField("elem" + index, mapper.toDataSchema(type.subType(index)));
            result.add(field);
        }
        return result;
    }
}
