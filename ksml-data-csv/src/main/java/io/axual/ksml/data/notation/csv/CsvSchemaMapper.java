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
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.StructField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;

public class CsvSchemaMapper implements DataSchemaMapper<String> {
    private static final CsvDataObjectMapper MAPPER = new CsvDataObjectMapper();

    @Override
    public DataSchema toDataSchema(String namespace, String name, String value) {
        // Convert CSV to internal DataObject format
        final var line = MAPPER.toDataObject(value);
        if (line instanceof DataList fieldNames) {
            return toDataSchema(namespace, name, fieldNames);
        }
        return null;
    }

    private DataSchema toDataSchema(String namespace, String name, DataList fieldNames) {
        final var fields = new ArrayList<StructField>();
        for (final var fieldName : fieldNames) {
            fields.add(new StructField(
                    fieldName.toString(DataObject.Printer.INTERNAL),
                    DataSchema.STRING_SCHEMA,
                    fieldName.toString(DataObject.Printer.INTERNAL),
                    NO_TAG,
                    true,
                    false,
                    new DataString("")));
        }
        return new StructSchema(namespace, name, "CSV schema", fields, false);
    }

    @Override
    public String fromDataSchema(DataSchema schema) {
        if (!(schema instanceof StructSchema structSchema)) return null;
        final var result = new StringBuilder();
        var first = true;
        for (final var field : structSchema.fields()) {
            if (!first) result.append(",");
            result.append(field.name());
            first = false;
        }
        return result.toString();
    }
}
