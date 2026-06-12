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
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.ObjectWriter;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvReadFeature;
import tools.jackson.dataformat.csv.CsvWriteFeature;

public class CsvDataObjectMapper implements DataObjectMapper<String> {
    private static final ObjectReader CSV_READER = new CsvMapper()
            .readerForArrayOf(String.class)
            .with(CsvReadFeature.WRAP_AS_ARRAY)
            .with(CsvReadFeature.SKIP_EMPTY_LINES)
            .with(CsvReadFeature.ALLOW_COMMENTS);
    private static final ObjectWriter CSV_WRITER = new CsvMapper()
            .writerFor(String[].class)
            .with(CsvWriteFeature.ESCAPE_CONTROL_CHARS_WITH_ESCAPE_CHAR)
            .with(CsvWriteFeature.ESCAPE_QUOTE_CHAR_WITH_ESCAPE_CHAR)
            .with(CsvWriteFeature.ALWAYS_QUOTE_STRINGS)
            .without(CsvWriteFeature.WRITE_LINEFEED_AFTER_LAST_ROW);
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
        } catch (JacksonException e) {
            throw new DataException("Could not parse CSV", e);
        }
        return new DataList(DataString.DATATYPE);
    }

    private DataStruct convertLineToDataStruct(String[] line, StructSchema schema) {
        // Convert the line to a DataStruct with given schema.
        //
        // Design choice — lenient CSV parsing:
        // 1. If the row is SHORTER than the schema (e.g. schema has 3 fields, row has 2),
        //    trailing missing columns are silently treated as "" for required fields and
        //    null for optional fields. The row is NOT rejected.
        // 2. If a required column is EMPTY in the row, it is stored as the empty string ""
        //    rather than rejected.
        //
        // This is intentional: CSV is a notoriously fuzzy format and many real-world feeds
        // ship truncated or partially-empty rows. Throwing here would break pipelines that
        // currently tolerate these inputs. Callers that need strict-schema enforcement should
        // validate the DataStruct downstream (e.g. with a custom filter step) rather than
        // relying on the CSV mapper to do it.
        final var result = new DataStruct(schema);
        for (int index = 0; index < schema.fields().size(); index++) {
            final var field = schema.field(index);
            final var lineValue = index < line.length ? line[index] : null;
            final String value;
            if (lineValue != null && !lineValue.isEmpty()) {
                value = lineValue;
            } else {
                value = field.required() ? "" : null;
            }
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
        return convertUtil.convertStringToDataObject(SCHEMA_TO_TYPE_MAPPER.fromDataSchema(schema), value, false);
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
        } catch (JacksonException e) {
            throw new DataException("Could not write CSV", e);
        }
    }
}
