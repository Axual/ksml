package io.axual.ksml.data.notation.csv;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.StructType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.List;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CsvDataObjectConverter}, in particular the structured-CSV-to-String
 * direction, which previously never fired because it compared a {@link io.axual.ksml.data.type.UnionType}
 * to a {@link ListType}/{@link StructType} with {@code equals}, which can never match.
 */
@DisplayName("CsvDataObjectConverter - CSV <-> DataObject notation conversions")
class CsvDataObjectConverterTest {

    private final CsvDataObjectConverter converter = new CsvDataObjectConverter();

    @Test
    @DisplayName("Converts a schema-less DataList to a properly CSV-escaped DataString")
    void convertsDataListToString() throws Exception {
        var list = new DataList(DataString.DATATYPE);
        list.add(DataString.from("value with, comma"));
        list.add(DataString.from("plain"));

        var result = converter.convert(list, DataString.DATATYPE);

        assertThat(result).isInstanceOf(DataString.class);
        var csv = ((DataString) result).value();

        // Real CSV output, not DataList's bracketed "[a, b]" toString()
        assertThat(csv).doesNotContain("[").doesNotContain("]");
        try (var parser = CSVParser.parse(new StringReader(csv), CSVFormat.DEFAULT)) {
            var records = parser.getRecords();
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().get(0)).isEqualTo("value with, comma");
            assertThat(records.getFirst().get(1)).isEqualTo("plain");
        }
    }

    @Test
    @DisplayName("Converts a DataStruct to a properly CSV-escaped DataString")
    void convertsDataStructToString() throws Exception {
        var schema = new StructSchema("io.axual.test", "Simple", "doc", List.of(
                new StructSchema.Field("name", DataSchema.STRING_SCHEMA, "name", NO_TAG, true, false, null),
                new StructSchema.Field("city", DataSchema.STRING_SCHEMA, "city", NO_TAG, true, false, null)),
                false);
        var struct = new DataStruct(schema);
        struct.put("name", DataString.from("Alice"));
        struct.put("city", DataString.from("Amsterdam"));

        var result = converter.convert(struct, DataString.DATATYPE);

        assertThat(result).isInstanceOf(DataString.class);
        var csv = ((DataString) result).value();
        try (var parser = CSVParser.parse(new StringReader(csv), CSVFormat.DEFAULT)) {
            var records = parser.getRecords();
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().get(0)).isEqualTo("Alice");
            assertThat(records.getFirst().get(1)).isEqualTo("Amsterdam");
        }
    }

    @Test
    @DisplayName("Converts a CSV DataString into a structured DataList")
    void convertsStringToDataList() {
        var result = converter.convert(DataString.from("\"a\",\"b\",\"c\""), new ListType());

        assertThat(result).isInstanceOf(DataList.class);
        var list = (DataList) result;
        assertThat(list.size()).isEqualTo(3);
        assertThat(list.get(0)).hasToString("a");
        assertThat(list.get(1)).hasToString("b");
        assertThat(list.get(2)).hasToString("c");
    }

    @Test
    @DisplayName("Returns null when no conversion is possible")
    void returnsNullForUnsupportedConversion() {
        // A DataString to a non-CSV structural type (not List/Struct/Union) is not handled here
        assertThat(converter.convert(DataString.from("42"), new SimpleType(Integer.class, "int"))).isNull();

        // A non-CSV value type (e.g. a plain DataString) converted to a DataString is not this converter's job
        assertThat(converter.convert(DataString.from("42"), DataString.DATATYPE)).isNull();
    }
}
