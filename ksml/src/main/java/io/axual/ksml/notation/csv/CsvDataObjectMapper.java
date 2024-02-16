package io.axual.ksml.notation.csv;

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

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.mapper.DataTypeSchemaMapper;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;

public class CsvDataObjectMapper implements DataObjectMapper<String> {
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();

    @Override
    public DataObject toDataObject(DataType expected, String value) {
        if (value != null) {
            try {
                var in = new StringReader(value);
                var reader = new CSVReader(in);
                var lines = reader.readAll();
                if (lines.size() > 0) {
                    if (expected instanceof StructType structType && structType.schema() != null)
                        return convertToStruct(lines.get(0), structType.schema());
                    return convertToList(lines.get(0));
                }
            } catch (IOException | CsvException e) {
                throw FatalError.executionError("Could not parse CSV", e);
            }
        }
        return new DataList(DataString.DATATYPE);
    }

    private DataStruct convertToStruct(String[] line, StructSchema schema) {
        // Convert the line with its elements to a Struct with given schema
        var result = new DataStruct(schema);
        for (int index = 0; index < schema.fields().size(); index++) {
            var field = schema.field(index);
            var value = index < line.length ? line[index] : (field.defaultValue() != null ? field.defaultValue().value() : null);
            result.put(field.name(), NATIVE_MAPPER.toDataObject(DataTypeSchemaMapper.schemaToDataType(field.schema()), value));
        }
        return result;
    }

    private DataList convertToList(String[] elements) {
        // Convert the line to a list of Strings
        var result = new DataList(DataString.DATATYPE);
        for (String element : elements) {
            result.add(DataString.from(element));
        }
        return result;
    }

    @Override
    public String fromDataObject(DataObject value) {
        if (value instanceof DataStruct valueStruct)
            return convertFromStruct(valueStruct);
        if (value instanceof DataList valueList)
            return convertFromList(valueList);
        return null;
    }

    private String convertFromStruct(DataStruct value) {
        if (value.type().schema() != null) {
            // Convert from a Struct with a schema --> compose fields by order in the schema
            var schema = value.type().schema();
            var line = new String[schema.fields().size()];
            for (var index = 0; index < schema.fields().size(); index++)
                line[index] = value.get(schema.field(index).name()).toString();
            return convertToCsv(line);
        } else {
            // Convert from a Struct without a schema --> compose fields alphabetically
            var line = new String[value.size()];
            var index = 0;
            for (var entry : value.entrySet())
                line[index++] = entry.getValue().toString();
            return convertToCsv(line);
        }
    }

    private String convertFromList(DataList value) {
        var line = new String[value.size()];
        for (var index = 0; index < value.size(); index++)
            line[index] = value.get(index).toString();
        return convertToCsv(line);
    }

    private String convertToCsv(String[] line) {
        var csv = new ArrayList<String[]>();
        csv.add(line);
        var out = new StringWriter();
        var writer = new CSVWriter(out);
        writer.writeAll(csv);
        return out.toString();
    }
}
