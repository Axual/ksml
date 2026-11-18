package io.axual.ksml.data.notation.protobuf;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
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
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Records written with another schema version than the local {@code .proto} of the stream. Each vendor module runs
 * these tests with its own descriptor mapper and registry library.
 */
abstract class AbstractProtobufUnknownFieldsTest {
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

    private ProtobufDataObjectMapper mapper;

    /**
     * @return the descriptor mapper of the vendor
     */
    protected abstract ProtobufFileElementDescriptorMapper descriptorMapper();

    /**
     * @param proto       the schema text
     * @param messageName the name of a message in the schema
     * @return the descriptor of the message, as the registry library of the vendor parses a writer schema
     */
    protected abstract Descriptors.Descriptor writerDescriptor(String proto, String messageName);

    @BeforeEach
    void setUp() {
        mapper = new ProtobufDataObjectMapper(descriptorMapper());
    }

    private static StructSchema localSchema(String proto) {
        return new ProtobufFileElementSchemaMapper(new NativeDataObjectMapper(), new DataTypeDataSchemaMapper())
                .toDataSchema(null, "Ev", ProtoParser.Companion.parse(Location.get(""), proto));
    }

    private DynamicMessage message(String proto, String text) throws TextFormat.ParseException {
        final var builder = DynamicMessage.newBuilder(writerDescriptor(proto, "Ev"));
        TextFormat.merge(text, builder);
        return builder.build();
    }

    // The message as a consumer with the given schema reads the bytes that KSML writes
    private DynamicMessage writeAndRead(DataStruct struct, String proto) throws Exception {
        final var written = (Message) mapper.fromDataObject(struct);
        return DynamicMessage.parseFrom(writerDescriptor(proto, "Ev"), written.toByteString());
    }

    private DataStruct read(String localProto, Message message) {
        return (DataStruct) mapper.toDataObject(new StructType(localSchema(localProto)), message);
    }

    private static String text(Message message) {
        return TextFormat.printer().shortDebugString(message);
    }

    @Test
    @DisplayName("A record with fields that the local schema does not have is read, without those fields")
    void readsRecordWithNewerFields() throws Exception {
        final var result = read(LOCAL_PROTO, message(NEWER_PROTO, NEWER_RECORD));

        assertThat(result.entrySet()).extracting(Map.Entry::getKey).containsExactlyInAnyOrder("name", "inner", "items");
        assertThat(result.get("name")).isEqualTo(new DataString("n1"));
        final var inner = (DataStruct) result.get("inner");
        assertThat(inner.entrySet()).extracting(Map.Entry::getKey).containsExactly("a");
        assertThat(inner.get("a")).isEqualTo(new DataString("x"));
        final var item = (DataStruct) ((DataList) result.get("items")).get(0);
        assertThat(item.entrySet()).extracting(Map.Entry::getKey).containsExactly("a");
        assertThat(item.get("a")).isEqualTo(new DataString("y"));
    }

    @Test
    @DisplayName("A record written with the local schema is read as before")
    void readsRecordWithLocalSchema() throws Exception {
        final var result = read(LOCAL_PROTO, message(LOCAL_PROTO, "name: 'n1' inner { a: 'x' }"));

        assertThat(result.entrySet()).extracting(Map.Entry::getKey).containsExactlyInAnyOrder("name", "inner");
        assertThat(((DataStruct) result.get("inner")).get("a")).isEqualTo(new DataString("x"));
        assertThat(result.unknownFields()).isNull();
    }

    @Test
    @DisplayName("Fields that the local schema does not have are written back unchanged, also in nested messages")
    void writesUnknownFieldsBack() throws Exception {
        final var original = message(NEWER_PROTO, NEWER_RECORD);

        final var written = (Message) mapper.fromDataObject(read(LOCAL_PROTO, original));

        assertThat(written.toByteArray()).isEqualTo(original.toByteArray());
        assertThat(text(writeAndRead(read(LOCAL_PROTO, original), NEWER_PROTO))).isEqualTo(text(original));
    }

    @Test
    @DisplayName("A record built again from its fields has no unknown fields")
    void newRecordHasNoUnknownFields() throws Exception {
        final var read = read(LOCAL_PROTO, message(NEWER_PROTO, "name: 'n1' extra: 'more'"));
        final var rebuilt = new DataStruct(read.type().schema());
        read.forEach(rebuilt::put);

        assertThat(text(writeAndRead(rebuilt, NEWER_PROTO))).isEqualTo("name: \"n1\"");
    }

    @Test
    @DisplayName("Unknown fields of a record that has the local schema are kept, like in a record that KSML wrote")
    void keepsUnknownFieldsOfRecordWithLocalSchema() throws Exception {
        final var original = message(NEWER_PROTO, NEWER_RECORD);
        final var withLocalSchema = DynamicMessage.parseFrom(writerDescriptor(LOCAL_PROTO, "Ev"), original.toByteString());

        final var result = read(LOCAL_PROTO, withLocalSchema);

        assertThat(result.entrySet()).extracting(Map.Entry::getKey).containsExactlyInAnyOrder("name", "inner", "items");
        assertThat(text(writeAndRead(result, NEWER_PROTO))).isEqualTo(text(original));
    }

    @Test
    @DisplayName("Unknown fields of a record are shown when the local schema has them, like in a later app with a newer schema")
    void showsUnknownFieldsThatTheLocalSchemaHas() throws Exception {
        final var original = message(NEWER_PROTO, NEWER_RECORD);
        // Written by an app with the older schema, which kept the new fields as unknown fields
        final var withOlderSchema = DynamicMessage.parseFrom(writerDescriptor(LOCAL_PROTO, "Ev"), original.toByteString());

        final var result = read(NEWER_PROTO, withOlderSchema);

        assertThat(result.get("extra")).isEqualTo(new DataString("more"));
        assertThat(((DataStruct) result.get("inner")).get("b")).isEqualTo(new DataInteger(7));
        assertThat(result.unknownFields()).isNull();
        assertThat(text(writeAndRead(result, NEWER_PROTO))).isEqualTo(text(original));
    }

    @Test
    @DisplayName("Fields with other numbers than in the local schema are read by name")
    void readsByNameWhenFieldNumbersDiffer() throws Exception {
        final var writer = "syntax = \"proto3\"; package unknown.test; message Ev { string name = 1; int64 ts = 2; string value = 3; }";
        final var local = "syntax = \"proto3\"; package unknown.test; message Ev { int64 ts = 1; string value = 2; string name = 3; }";

        final var result = read(local, message(writer, "name: 'N' ts: 5 value: 'V'"));

        assertThat(result.get("name")).isEqualTo(new DataString("N"));
        assertThat(result.get("ts")).isEqualTo(new DataLong(5L));
        assertThat(result.get("value")).isEqualTo(new DataString("V"));
        assertThat(result.unknownFields()).isNull();
        assertThat(text(writeAndRead(result, local))).isEqualTo("ts: 5 value: \"V\" name: \"N\"");
    }

    @Test
    @DisplayName("When field numbers differ, new fields and unknown fields of the record are dropped")
    void dropsNewFieldsWhenFieldNumbersDiffer() throws Exception {
        final var newest = "syntax = \"proto3\"; package unknown.test; message Ev { string name = 1; int32 n = 2; string extra = 3; string more = 9; }";
        final var writer = "syntax = \"proto3\"; package unknown.test; message Ev { string name = 1; int32 n = 2; string extra = 3; }";
        final var local = "syntax = \"proto3\"; package unknown.test; message Ev { int32 n = 1; string name = 2; }";
        // A record with the writer schema that kept field 9 of a newer schema as an unknown field
        final var withUnknownField = DynamicMessage.parseFrom(writerDescriptor(writer, "Ev"),
                message(newest, "name: 'N' n: 7 extra: 'X' more: 'M'").toByteString());

        final var result = read(local, withUnknownField);

        assertThat(result.entrySet()).extracting(Map.Entry::getKey).containsExactlyInAnyOrder("name", "n");
        assertThat(result.get("name")).isEqualTo(new DataString("N"));
        assertThat(result.get("n")).isEqualTo(new DataInteger(7));
        assertThat(result.unknownFields()).isNull();
        assertThat(text(writeAndRead(result, local))).isEqualTo("n: 7 name: \"N\"");
    }

    @Test
    @DisplayName("A renamed field is matched by name, so the field with the old name is empty")
    void readsRenamedFieldByName() throws Exception {
        final var writer = "syntax = \"proto3\"; package unknown.test; message Ev { string full_name = 1; int32 n = 2; }";
        final var local = "syntax = \"proto3\"; package unknown.test; message Ev { string name = 1; int32 n = 2; }";

        final var result = read(local, message(writer, "full_name: 'N' n: 7"));

        assertThat(result.entrySet()).extracting(Map.Entry::getKey).containsExactly("n");
        assertThat(result.unknownFields()).isNull();
    }
}
