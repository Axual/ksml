package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.EnumType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DataObjectTest {
    private static final EnumType ENUM_TYPE = new EnumType(new EnumSchema(List.of(new EnumSchema.Symbol("A"))));

    static Stream<Arguments> noneValues() {
        return Stream.of(
                Arguments.of("null", null),
                Arguments.of("DataNull", DataNull.INSTANCE),
                Arguments.of("typed null string", new DataString()),
                Arguments.of("typed null bytes", new DataBytes()),
                Arguments.of("typed null enum", new DataEnum(ENUM_TYPE, null)),
                Arguments.of("null list", new DataList(DataType.UNKNOWN, true)),
                Arguments.of("null map", new DataMap(DataType.UNKNOWN, true)),
                Arguments.of("null struct", new DataStruct(null, true)));
    }

    static Stream<Arguments> values() {
        return Stream.of(
                Arguments.of("empty string", new DataString("")),
                Arguments.of("empty bytes", new DataBytes(new byte[0])),
                Arguments.of("enum", new DataEnum(ENUM_TYPE, "A")),
                Arguments.of("empty list", new DataList(DataType.UNKNOWN)),
                Arguments.of("empty struct", new DataStruct()),
                Arguments.of("tuple with a DataNull", new DataTuple(DataNull.INSTANCE)),
                Arguments.of("not a DataObject", "native"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("noneValues")
    @DisplayName("None, in all its forms, is None and becomes null")
    void none(String name, Object none) {
        assertThat(DataObject.isNone(none)).isTrue();
        assertThat(DataObject.nullIfNone(none)).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("values")
    @DisplayName("A value, also an empty one, is not None and is returned as is")
    void value(String name, Object value) {
        assertThat(DataObject.isNone(value)).isFalse();
        assertThat(DataObject.nullIfNone(value)).isSameAs(value);
    }
}
