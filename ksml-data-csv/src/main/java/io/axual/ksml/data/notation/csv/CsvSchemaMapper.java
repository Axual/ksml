package io.axual.ksml.data.notation.csv;

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

import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;
import java.util.List;

import static io.axual.ksml.data.schema.DataField.NO_INDEX;

public class CsvSchemaMapper implements DataSchemaMapper<String> {
    private static final CsvDataObjectMapper MAPPER = new CsvDataObjectMapper();

    @Override
    public DataSchema toDataSchema(String namespace, String name, String value) {
        // Convert CSV to internal DataObject format
        var line = MAPPER.toDataObject(value);
        if (line instanceof DataList fieldNames) {
            return toDataSchema(namespace, name, fieldNames);
        }
        return null;
    }

    private DataSchema toDataSchema(String namespace, String name, DataList fieldNames) {
        List<DataField> fields = new ArrayList<>();
        for (var fieldName : fieldNames) {
            fields.add(new DataField(
                    fieldName.toString(DataObject.Printer.INTERNAL),
                    DataSchema.create(DataSchema.Type.STRING),
                    fieldName.toString(DataObject.Printer.INTERNAL),
                    NO_INDEX,
                    true,
                    false,
                    new DataValue("")));
        }
        return new StructSchema(namespace, name, "CSV schema", fields);
    }

    @Override
    public String fromDataSchema(DataSchema schema) {
        if (!(schema instanceof StructSchema structSchema)) return null;
        var result = new StringBuilder();
        var first = true;
        for (var field : structSchema.fields()) {
            if (!first) result.append(",");
            result.append(field.name());
            first = false;
        }
        return result.toString();
    }
}
