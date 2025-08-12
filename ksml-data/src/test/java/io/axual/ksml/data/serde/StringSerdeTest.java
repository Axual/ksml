package io.axual.ksml.data.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.mapper.StringDataObjectMapper;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StringSerdeTest {
    private static final String TOPIC = "topic";

    @Test
    @DisplayName("serialize maps Java String to DataString and delegates to Kafka StringSerializer")
    void serializerHappyPath() {
        var nativeMapper = new NativeDataObjectMapper();
        var serde = new StringSerde(nativeMapper);

        var serialized = serde.serializer().serialize(TOPIC, "hello");
        var rawDeserialized = new StringDeserializer().deserialize(TOPIC, serialized);
        assertThat(rawDeserialized).isEqualTo("hello");
    }

    @Test
    @DisplayName("deserialize returns DataString via StringDataObjectMapper")
    void deserializerHappyPath() {
        var nativeMapper = new NativeDataObjectMapper();
        var serde = new StringSerde(nativeMapper);

        var bytes = new StringSerializer().serialize(TOPIC, "world");
        Object dataObject = serde.deserializer().deserialize(TOPIC, bytes);
        assertThat(dataObject)
                .isInstanceOf(DataString.class)
                .extracting(o -> ((DataString) o).value())
                .isEqualTo("world");
    }

    @Test
    @DisplayName("serializer throws DataException when native value can not be mapped to DataString type")
    void serializerThrowsForWrongValue() {
        var nativeMapper = new NativeDataObjectMapper();
        var serde = new StringSerde(nativeMapper);

        assertThatThrownBy(() -> serde.serializer().serialize(TOPIC, new Exception("test")))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Can not convert value to DataObject: Exception");
    }

    @Test
    @DisplayName("deserializer throws DataException if stringMapper produces wrong type")
    void deserializerThrowsIfMapperProducesWrongType() {
        var nativeMapper = new NativeDataObjectMapper();
        DataObjectMapper<String> badStringMapper = new StringDataObjectMapper() {
            @Override
            public DataObject toDataObject(DataType expected, String value) {
                // deliberately return DataInteger to trigger type mismatch against expected DataString
                return new DataInteger(5);
            }
        };
        var serde = new StringSerde(nativeMapper, badStringMapper, DataString.DATATYPE);

        var bytes = new StringSerializer().serialize(TOPIC, "oops");
        assertThatThrownBy(() -> serde.deserializer().deserialize(TOPIC, bytes))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Wrong type retrieved from state store")
                .hasMessageContaining(DataString.DATATYPE.toString())
                .hasMessageContaining(DataInteger.DATATYPE.toString());
    }
}
