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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.DataTypeSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;

import java.io.IOException;

public class CsvDataObjectMapper implements DataObjectMapper<String> {
    private static final ObjectReader CSV_READER = new CsvMapper()
            .readerForArrayOf(String.class)
            .with(CsvParser.Feature.WRAP_AS_ARRAY)
            .with(CsvParser.Feature.SKIP_EMPTY_LINES)
            .with(CsvParser.Feature.ALLOW_COMMENTS);
    private static final ObjectWriter CSV_WRITER = new CsvMapper()
            .writerFor(String[].class)
            .with(CsvGenerator.Feature.ESCAPE_CONTROL_CHARS_WITH_ESCAPE_CHAR)
            .with(CsvGenerator.Feature.ESCAPE_QUOTE_CHAR_WITH_ESCAPE_CHAR)
            .with(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS);
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final DataTypeSchemaMapper SCHEMA_MAPPER = new DataTypeSchemaMapper();

    @Override
    public DataObject toDataObject(DataType expected, String value) {
        try (final var iterator = CSV_READER.readValues(value)) {
            while (iterator.hasNextValue()) {
                final var line = iterator.nextValue();
                if (line instanceof String[] values) {
                    if (expected instanceof StructType structType && structType.schema() != null)
                        return convertToStruct(values, structType.schema());
                    return convertToList(values);
                }
            }
        } catch (IOException e) {
            throw new ExecutionException("Could not parse CSV", e);
        }
        return new DataList(DataString.DATATYPE);
    }

    private DataStruct convertToStruct(String[] line, StructSchema schema) {
        // Convert the line with its elements to a Struct with given schema
        var result = new DataStruct(schema);
        for (int index = 0; index < schema.fields().size(); index++) {
            var field = schema.field(index);
            var value = index < line.length ? line[index] : (field.defaultValue() != null ? field.defaultValue().value() : null);
            result.put(field.name(), NATIVE_MAPPER.toDataObject(SCHEMA_MAPPER.fromDataSchema(field.schema()), value));
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
        try {
            return CSV_WRITER.writeValueAsString(line);
        } catch (JsonProcessingException e) {
            throw new ExecutionException("Could not write CSV", e);
        }
    }
}
