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
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.exception.ExecutionException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
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
