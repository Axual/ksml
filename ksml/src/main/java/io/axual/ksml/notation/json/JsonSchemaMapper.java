package io.axual.ksml.notation.json;

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
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.AnySchema;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.StructSchema;

import java.util.ArrayList;
import java.util.List;

public class JsonSchemaMapper implements DataSchemaMapper<String> {
    private static final JsonDataObjectMapper MAPPER = new JsonDataObjectMapper();
    private static final String TITLE_NAME = "title";
    private static final String DESCRIPTION_NAME = "description";
    private static final String TYPE_NAME = "type";
    private static final String PROPERTIES_NAME = "properties";
    private static final String ITEMS_NAME = "items";

    @Override
    public DataSchema toDataSchema(String name, String value) {
        // Convert JSON to internal DataObject format
        var schema = MAPPER.toDataObject(value);
        if (schema instanceof DataStruct schemaStruct) {
            return toDataSchema(schemaStruct);
        }
        return null;
    }

    private DataSchema toDataSchema(DataStruct schema) {
        var title = schema.getAsString(TITLE_NAME);
        var doc = schema.getAsString(DESCRIPTION_NAME);
        var properties = schema.get(PROPERTIES_NAME);
        List<DataField> fields = null;
        if (properties instanceof DataStruct propertiesStruct)
            fields = convertFields(propertiesStruct);
        return new StructSchema(null, title != null ? title.value() : null, doc != null ? doc.value() : null, fields);
    }

    private List<DataField> convertFields(DataStruct properties) {
        var result = new ArrayList<DataField>();
        for (var entry : properties.entrySet()) {
            var name = entry.getKey();
            var spec = entry.getValue();
            if (spec instanceof DataStruct specStruct) {
                var doc = specStruct.getAsString(DESCRIPTION_NAME);
                var field = new DataField(name, convertType(specStruct), doc != null ? doc.value() : null, null, DataField.Order.ASCENDING);
                result.add(field);
            }
        }
        return result;
    }

    private DataSchema convertType(DataStruct specStruct) {
        var type = specStruct.getAsString(TYPE_NAME);
        if (type != null) {
            switch (type.value()) {
                case "null":
                    return DataSchema.create(DataSchema.Type.NULL);
                case "boolean":
                    return DataSchema.create(DataSchema.Type.BOOLEAN);
                case "number":
                    return DataSchema.create(DataSchema.Type.LONG);
                case "array":
                    return toListSchema(specStruct);
                case "object":
                    return toDataSchema(specStruct);
                case "string":
                    return DataSchema.create(DataSchema.Type.STRING);
            }
        }
        return AnySchema.INSTANCE;
    }

    private ListSchema toListSchema(DataStruct spec) {
        var items = spec.get(ITEMS_NAME);
        if (items instanceof DataStruct itemsStruct) {
            var valueSchema = convertType(itemsStruct);
            return new ListSchema(valueSchema);
        }
        return new ListSchema(AnySchema.INSTANCE);
    }

    @Override
    public String fromDataSchema(DataSchema schema) {
        return null;
    }
}
