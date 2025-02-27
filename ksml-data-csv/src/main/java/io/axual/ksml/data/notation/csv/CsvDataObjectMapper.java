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
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.util.ConvertUtil;

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
            .with(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS)
            .without(CsvGenerator.Feature.WRITE_LINEFEED_AFTER_LAST_ROW);
    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();
    private static final DataTypeDataSchemaMapper SCHEMA_TO_TYPE_MAPPER = new DataTypeDataSchemaMapper();
    private final ConvertUtil convertUtil;

    public CsvDataObjectMapper() {
        convertUtil = new ConvertUtil(NATIVE_MAPPER, SCHEMA_TO_TYPE_MAPPER);
    }

    @Override
    public DataObject toDataObject(DataType expected, String value) {
        try (final var iterator = CSV_READER.readValues(value)) {
            while (iterator.hasNextValue()) {
                final var line = iterator.nextValue();
                if (line instanceof String[] values) {
                    if (expected instanceof StructType structType && structType.schema() != null)
                        return convertLineToDataStruct(values, structType.schema());
                    return convertLineToDataList(values);
                }
            }
        } catch (IOException e) {
            throw new DataException("Could not parse CSV", e);
        }
        return new DataList(DataString.DATATYPE);
    }

    private DataStruct convertLineToDataStruct(String[] line, StructSchema schema) {
        // Convert the line to a DataStruct with given schema
        final var result = new DataStruct(schema);
        for (int index = 0; index < schema.fields().size(); index++) {
            final var field = schema.field(index);
            final var lineValue = index < line.length ? line[index] : null;
            final var value = lineValue != null && !lineValue.isEmpty()
                    ? lineValue
                    : field.required()
                    ? ""
                    : null;
            if (value != null) result.putIfNotNull(field.name(), convertStringToDataObject(field.schema(), value));
        }
        return result;
    }

    private DataList convertLineToDataList(String[] line) {
        // Convert the line to a DataList of DataStrings
        final var result = new DataList(DataString.DATATYPE);
        for (final var element : line) result.add(DataString.from(element));
        return result;
    }

    private DataObject convertStringToDataObject(DataSchema schema, String value) {
        return convertUtil.convertStringToDataObject(SCHEMA_TO_TYPE_MAPPER.fromDataSchema(schema), value);
    }

    @Override
    public String fromDataObject(DataObject value) {
        if (value instanceof DataStruct valueStruct)
            return convertDataStructToLine(valueStruct);
        if (value instanceof DataList valueList)
            return convertDataListToLine(valueList);
        return null;
    }

    private String convertDataStructToLine(DataStruct value) {
        if (value.type().schema() != null) {
            // Convert from a Struct with a schema --> compose fields by order in the schema
            final var schema = value.type().schema();
            final var line = new String[schema.fields().size()];
            for (var index = 0; index < schema.fields().size(); index++) {
                final var fieldName = schema.field(index).name();
                final var fieldValue = value.get(fieldName);
                line[index] = fieldValue != null ? fieldValue.toString() : "";
            }
            return convertStringsToLine(line);
        } else {
            // Convert from a Struct without a schema --> compose fields alphabetically
            final var line = new String[value.size()];
            var index = 0;
            for (var entry : value.entrySet())
                line[index++] = entry.getValue().toString();
            return convertStringsToLine(line);
        }
    }

    private String convertDataListToLine(DataList value) {
        final var line = new String[value.size()];
        for (var index = 0; index < value.size(); index++)
            line[index] = value.get(index).toString();
        return convertStringsToLine(line);
    }

    private String convertStringsToLine(String[] line) {
        try {
            return CSV_WRITER.writeValueAsString(line);
        } catch (JsonProcessingException e) {
            throw new DataException("Could not write CSV", e);
        }
    }
}
