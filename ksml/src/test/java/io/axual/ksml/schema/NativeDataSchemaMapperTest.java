package io.axual.ksml.schema;

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

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataEnum;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.exception.ExecutionException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeDataSchemaMapperTest {

    private final NativeDataSchemaMapper mapper = new NativeDataSchemaMapper();

    @Test
    void booleanDefaultValue_isEncodedCorrectly() {
        var field = new StructSchema.Field("active", DataSchema.BOOLEAN_SCHEMA, null, 0, false, false, new DataBoolean(true));
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        var result = (Map<?, ?>) mapper.fromDataSchema(schema);

        var fields = (List<?>) result.get("fields");
        var fieldMap = (Map<?, ?>) fields.get(0);
        assertThat(fieldMap.get("defaultValue")).isEqualTo(true);
    }

    @Test
    void booleanFalseDefaultValue_isEncodedCorrectly() {
        var field = new StructSchema.Field("deleted", DataSchema.BOOLEAN_SCHEMA, null, 0, false, false, new DataBoolean(false));
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        var result = (Map<?, ?>) mapper.fromDataSchema(schema);

        var fields = (List<?>) result.get("fields");
        var fieldMap = (Map<?, ?>) fields.get(0);
        assertThat(fieldMap.get("defaultValue")).isEqualTo(false);
    }

    @Test
    void emptyListDefaultValue_isEncodedAsEmptyList() {
        var field = new StructSchema.Field("tags", new ListSchema(DataSchema.STRING_SCHEMA), null, 0, false, false, new DataList());
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        var result = (Map<?, ?>) mapper.fromDataSchema(schema);

        var fields = (List<?>) result.get("fields");
        var fieldMap = (Map<?, ?>) fields.get(0);
        assertThat(fieldMap.get("defaultValue")).isEqualTo(List.of());
    }

    @Test
    void listDefaultValue_withStringElements_isEncodedCorrectly() {
        var list = new DataList();
        list.add(new DataString("a"));
        list.add(new DataString("b"));
        var field = new StructSchema.Field("tags", new ListSchema(DataSchema.STRING_SCHEMA), null, 0, false, false, list);
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        var result = (Map<?, ?>) mapper.fromDataSchema(schema);

        var fields = (List<?>) result.get("fields");
        var fieldMap = (Map<?, ?>) fields.get(0);
        assertThat(fieldMap.get("defaultValue")).isEqualTo(List.of("a", "b"));
    }

    @Test
    void enumDefaultValue_isEncodedAsString() {
        var enumSchema = new EnumSchema(null, "SensorType", null,
                List.of(EnumSchema.Symbol.of("AREA"), EnumSchema.Symbol.of("TEMPERATURE")));
        var enumType = new EnumType(enumSchema);
        var field = new StructSchema.Field("kind", enumSchema, null, 0, false, false, new DataEnum(enumType, "AREA"));
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        var result = (Map<?, ?>) mapper.fromDataSchema(schema);

        var fields = (List<?>) result.get("fields");
        var fieldMap = (Map<?, ?>) fields.get(0);
        assertThat(fieldMap.get("defaultValue")).isEqualTo("AREA");
    }

    @Test
    void emptyMapDefaultValue_isEncodedAsEmptyMap() {
        var field = new StructSchema.Field("props", DataSchema.STRING_SCHEMA, null, 0, false, false, new DataMap());
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        var result = (Map<?, ?>) mapper.fromDataSchema(schema);

        var fields = (List<?>) result.get("fields");
        var fieldMap = (Map<?, ?>) fields.get(0);
        assertThat(fieldMap.get("defaultValue")).isEqualTo(Map.of());
    }

    @Test
    void mapDefaultValue_withStringValues_isEncodedCorrectly() {
        var map = new DataMap();
        map.put("key1", new DataString("val1"));
        map.put("key2", new DataString("val2"));
        var field = new StructSchema.Field("props", DataSchema.STRING_SCHEMA, null, 0, false, false, map);
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        var result = (Map<?, ?>) mapper.fromDataSchema(schema);

        var fields = (List<?>) result.get("fields");
        var fieldMap = (Map<?, ?>) fields.get(0);
        assertThat(fieldMap.get("defaultValue")).isEqualTo(Map.of("key1", "val1", "key2", "val2"));
    }

    @Test
    void attributesField_withEmptyArrayDefault_doesNotThrow() {
        // Regression test: Avro schema with "Attributes": { "type": "array", "default": [] } crashed with DataList
        var attributeSchema = new StructSchema(null, "Attribute", null, List.of(
                new StructSchema.Field("AttributeName", DataSchema.STRING_SCHEMA, null, 0, false, false, null),
                new StructSchema.Field("AttributeValue", DataSchema.STRING_SCHEMA, null, 0, false, false, null)));
        var attributesField = new StructSchema.Field("Attributes", new ListSchema(attributeSchema),
                null, 0, false, false, new DataList());
        var schema = new StructSchema(null, "Asset", null, List.of(attributesField));

        assertThatNoException().isThrownBy(() -> mapper.fromDataSchema(schema));
    }

    @Test
    void unknownDefaultValueType_throwsWithTypeAndValueInMessage() {
        var unhandledDefault = new DataStruct();
        var field = new StructSchema.Field("broken", DataSchema.STRING_SCHEMA, null, 0, false, false, unhandledDefault);
        var schema = new StructSchema(null, "TestRecord", null, List.of(field));

        assertThatThrownBy(() -> mapper.fromDataSchema(schema))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("DataStruct")
                .hasMessageContaining(unhandledDefault.toString());
    }
}
