package io.axual.ksml.data.notation.string;

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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.mapper.StringDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StringNotationTest {
    private static class ConcreteStringNotation extends StringNotation {
        ConcreteStringNotation(NotationContext context,
                               String filenameExtension,
                               DataType defaultType,
                               Notation.Converter converter,
                               Notation.SchemaParser schemaParser,
                               DataObjectMapper<String> stringMapper) {
            super(context, filenameExtension, defaultType, converter, schemaParser, stringMapper);
        }
    }

    @Test
    @DisplayName("serde() returns a StringSerde that roundtrips DataString via configured mappers and configs are applied")
    void serdeRoundTripsDataString() {
        var nativeMapper = new NativeDataObjectMapper();
        var context = new NotationContext("string", nativeMapper);
        context.serdeConfigs().put("dummy", "config");

        var notation = new ConcreteStringNotation(
                context,
                ".txt",
                DataString.DATATYPE,
                (value, targetType) -> value,
                (ctx, name, str) -> null,
                new StringDataObjectMapper()
        );

        var serde = notation.serde(DataString.DATATYPE, true);

        var serialized = serde.serializer().serialize("topic", new DataString("hello"));
        var deserialized = serde.deserializer().deserialize("topic", new RecordHeaders(), serialized);

        assertThat(deserialized).isInstanceOf(DataString.class);
        assertThat(((DataString) deserialized).value()).isEqualTo("hello");
        assertThat(notation.filenameExtension()).isEqualTo(".txt");
        assertThat(notation.name()).isEqualTo("string");
    }
}
