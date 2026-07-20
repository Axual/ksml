package io.axual.ksml.data.notation.jsonschema.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - JSON Schema Apicurio
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

// Jackson 2 is used deliberately: Apicurio's JSON Schema deserializer returns a Jackson 2 JsonNode,
// which is exactly the input this mapper must bridge to KSML's Jackson 3 world.
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApicurioJsonSchemaDataObjectMapperTest {
    private static final ObjectMapper JACKSON2 = new ObjectMapper();

    private final ApicurioJsonSchemaDataObjectMapper mapper =
            new ApicurioJsonSchemaDataObjectMapper(new NativeDataObjectMapper());

    @Test
    @DisplayName("A null value maps to DataNull when no specific type is expected")
    void nullMapsToDataNull() {
        assertThat(mapper.toDataObject(null, null)).isInstanceOf(DataNull.class);
    }

    @Test
    @DisplayName("A Jackson 2 object node is bridged to a DataStruct")
    void jackson2NodeMapsToDataStruct() throws Exception {
        final var node = JACKSON2.readTree("{\"name\":\"sensor0\",\"value\":42}");

        final var result = mapper.toDataObject(new StructType(), node);

        assertThat(result).isInstanceOf(DataStruct.class);
        assertThat(((DataStruct) result).get("name")).isNotNull();
    }

    @Test
    @DisplayName("A DataObject converts back to a native Map")
    void fromDataObjectReturnsNative() throws Exception {
        final var node = JACKSON2.readTree("{\"name\":\"sensor0\"}");
        final var data = (DataObject) mapper.toDataObject(new StructType(), node);

        assertThat(mapper.fromDataObject(data)).isInstanceOf(Map.class);
    }

    @Test
    @DisplayName("A value whose text is not valid JSON throws a DataException")
    void invalidJsonThrows() {
        final var notJson = new Object() {
            @Override
            public String toString() {
                return "not-json{";
            }
        };

        assertThatThrownBy(() -> mapper.toDataObject(new StructType(), notJson))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Cannot convert value to DataObject");
    }
}
