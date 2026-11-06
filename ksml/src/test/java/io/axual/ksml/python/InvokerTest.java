package io.axual.ksml.python;

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

import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.axual.ksml.python.Invoker.nullIfNone;
import static org.assertj.core.api.Assertions.assertThat;

class InvokerTest {

    static Stream<Arguments> noneValues() {
        return Stream.of(
                Arguments.of("DataNull", DataNull.INSTANCE),
                Arguments.of("typed null string", new DataString()),
                Arguments.of("typed null integer", new DataInteger()),
                Arguments.of("null list", new DataList(DataType.UNKNOWN, true)),
                Arguments.of("null map", new DataMap(DataType.UNKNOWN, true)),
                Arguments.of("null struct", new DataStruct(null, true)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("noneValues")
    @DisplayName("None, in all its forms, becomes a real null")
    void noneBecomesNull(String name, DataObject none) {
        assertThat(nullIfNone(none)).isNull();
    }

    @Test
    @DisplayName("null stays null")
    void nullStaysNull() {
        assertThat(nullIfNone((DataObject) null)).isNull();
    }

    @Test
    @DisplayName("A value, also an empty one, is returned as is")
    void valueIsReturnedAsIs() {
        final var string = new DataString("a");
        final var emptyString = new DataString("");
        final var emptyList = new DataList(DataType.UNKNOWN);
        final var emptyStruct = new DataStruct();
        assertThat(nullIfNone(string)).isSameAs(string);
        assertThat(nullIfNone(emptyString)).isSameAs(emptyString);
        assertThat(nullIfNone(emptyList)).isSameAs(emptyList);
        assertThat(nullIfNone(emptyStruct)).isSameAs(emptyStruct);
    }

    @Test
    @DisplayName("Key and value of a KeyValue are converted separately")
    void keyValue() {
        final var value = new DataString("v");
        final var result = nullIfNone(new KeyValue<Object, Object>(new DataString(), value));
        assertThat(result.key).isNull();
        assertThat(result.value).isSameAs(value);
    }

    @Test
    @DisplayName("A key or value that is not a DataObject is returned as is")
    void notADataObject() {
        final var result = nullIfNone(new KeyValue<Object, Object>("native", "v"));
        assertThat(result.key).isEqualTo("native");
        assertThat(result.value).isEqualTo("v");
    }
}
