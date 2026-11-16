package io.axual.ksml.data.notation.protobuf.apicurio;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF Apicurio
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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.TextFormat;
import io.apicurio.registry.serde.protobuf.ProtobufSchemaParser;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.notation.protobuf.ProtobufDataObjectMapper;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Records written with a newer schema than the local {@code .proto} of the stream.
 */
class ProtobufUnknownFieldsTest {
    private static final String LOCAL_PROTO = """
            syntax = "proto3";
            package unknown.test;
            message Inner {
              string a = 1;
            }
            message Ev {
              string name = 1;
              Inner inner = 2;
              repeated Inner items = 3;
            }
            """;
    // A newer version: a new field in Ev and a new field in Inner
    private static final String NEWER_PROTO = """
            syntax = "proto3";
            package unknown.test;
            message Inner {
              string a = 1;
              int32 b = 2;
            }
            message Ev {
              string name = 1;
              Inner inner = 2;
              repeated Inner items = 3;
              string extra = 4;
            }
            """;
    private static final String NEWER_RECORD = "name: 'n1' inner { a: 'x' b: 7 } items { a: 'y' b: 8 } extra: 'more'";

    private final ProtobufDataObjectMapper mapper = new ProtobufDataObjectMapper(new ApicurioProtobufFileElementDescriptorMapper());
    private final StructSchema localSchema = (StructSchema) new ApicurioProtobufSchemaParser(new NativeDataObjectMapper(), new DataTypeDataSchemaMapper())
            .parse("test", "Ev", LOCAL_PROTO);

    private static Descriptors.Descriptor descriptor(String proto) {
        return new ProtobufSchemaParser<>().parseSchema(proto.getBytes(), Collections.emptyMap())
                .getFileDescriptor().findMessageTypeByName("Ev");
    }

    private static DynamicMessage message(String proto, String text) throws TextFormat.ParseException {
        final var builder = DynamicMessage.newBuilder(descriptor(proto));
        TextFormat.merge(text, builder);
        return builder.build();
    }

    private DataStruct read(DynamicMessage message) {
        return (DataStruct) mapper.toDataObject(new StructType(localSchema), message);
    }

    @Test
    @DisplayName("A record with fields that the local schema does not have is read, without those fields")
    void readsRecordWithNewerFields() throws TextFormat.ParseException {
        final var result = read(message(NEWER_PROTO, NEWER_RECORD));

        assertThat(result.type().schema()).isSameAs(localSchema);
        assertThat(result.entrySet()).extracting(Map.Entry::getKey).containsExactlyInAnyOrder("name", "inner", "items");
        assertThat(result.getAsString("name").value()).isEqualTo("n1");
        final var inner = (DataStruct) result.get("inner");
        assertThat(inner.entrySet()).extracting(Map.Entry::getKey).containsExactly("a");
        assertThat(inner.getAsString("a").value()).isEqualTo("x");
        final var item = (DataStruct) ((DataList) result.get("items")).get(0);
        assertThat(item.entrySet()).extracting(Map.Entry::getKey).containsExactly("a");
        assertThat(item.getAsString("a").value()).isEqualTo("y");
    }

    @Test
    @DisplayName("A record written with the local schema is read as before")
    void readsRecordWithLocalSchema() throws TextFormat.ParseException {
        final var result = read(message(LOCAL_PROTO, "name: 'n1' inner { a: 'x' }"));

        assertThat(result.entrySet()).extracting(Map.Entry::getKey).containsExactlyInAnyOrder("name", "inner");
        assertThat(((DataStruct) result.get("inner")).getAsString("a").value()).isEqualTo("x");
    }
}
