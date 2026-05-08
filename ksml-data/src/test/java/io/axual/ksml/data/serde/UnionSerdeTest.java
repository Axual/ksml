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
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.type.UnionType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnionSerdeTest {
    private static final String TOPIC = "topic";

    private static final DataType BYTE_TYPE = new SimpleType(Byte.class, "Byte");
    private static final DataType STR_TYPE = new SimpleType(String.class, "String");

    @Test
    @DisplayName("configure() propagates to all member serializers and deserializers")
    void configurePropagatesToMembers() {
        final var seenSerConfig = new AtomicReference<Map<String, ?>>();
        final var seenSerIsKey = new AtomicReference<Boolean>();
        final var seenDeserConfig = new AtomicReference<Map<String, ?>>();
        final var seenDeserIsKey = new AtomicReference<Boolean>();

        final SerdeSupplier spyingSerde = (type, isKey) -> new Serde<>() {
            @Override
            public Serializer<Object> serializer() {
                return new Serializer<>() {
                    @Override
                    public void configure(Map<String, ?> configs, boolean isKey) {
                        seenSerConfig.set(configs);
                        seenSerIsKey.set(isKey);
                    }

                    @Override
                    public byte[] serialize(String topic, Object data) {
                        return null;
                    }
                };
            }

            @Override
            public Deserializer<Object> deserializer() {
                return new Deserializer<>() {
                    @Override
                    public void configure(Map<String, ?> configs, boolean isKey) {
                        seenDeserConfig.set(configs);
                        seenDeserIsKey.set(isKey);
                    }

                    @Override
                    public Object deserialize(String topic, byte[] data) {
                        return null;
                    }
                };
            }
        };

        final var union = new UnionType(new UnionType.Member(new SimpleType(String.class, "String")));
        final var serde = new UnionSerde(union, false, spyingSerde);
        final var config = Map.of("a", 1);
        serde.configure(config, true);

        assertThat(seenSerConfig.get()).isEqualTo(config);
        assertThat(seenDeserConfig.get()).isEqualTo(config);
        assertThat(seenSerIsKey.get()).isTrue();
        assertThat(seenDeserIsKey.get()).isTrue();
    }

    @Test
    @DisplayName("serializer: null and DataNull serialize to null; native values routed by first compatible type")
    void serializeNullsAndNativeValues() {
        final var union = new UnionType(
                new UnionType.Member(STR_TYPE),
                new UnionType.Member(BYTE_TYPE)
        );
        final SerdeSupplier supplier = (type, isKey) -> {
            if (type.equals(STR_TYPE)) {
                var kafka = Serdes.String();
                return new Serde<>() {
                    @Override
                    public Serializer<Object> serializer() {
                        return (Serializer<Object>) (Serializer<?>) kafka.serializer();
                    }

                    @Override
                    public Deserializer<Object> deserializer() {
                        return (Deserializer<Object>) (Deserializer<?>) kafka.deserializer();
                    }
                };
            }
            if (type.equals(BYTE_TYPE)) return new ByteSerde();
            throw new IllegalArgumentException("unexpected type");
        };
        final var serde = new UnionSerde(union, false, supplier);

        // null and DataNull -> null bytes
        assertThat(serde.serializer().serialize(TOPIC, null)).isNull();
        assertThat(serde.serializer().serialize(TOPIC, DataNull.INSTANCE)).isNull();

        // String value goes to String serializer (first member)
        final var sBytes = serde.serializer().serialize(TOPIC, "hello");

        assertThat(new String(sBytes)).isEqualTo("hello");

        // Byte value matches the second member
        final var bBytes = serde.serializer().serialize(TOPIC, (byte) 0x7F);
        assertThat(bBytes).containsExactly((byte) 0x7F);
    }

    @Test
    @DisplayName("serializer throws DataException when no member type is compatible")
    void serializeUnsupportedTypeThrows() {
        var union = new UnionType(new UnionType.Member(BYTE_TYPE));
        SerdeSupplier supplier = (type, isKey) -> new ByteSerde();
        var serde = new UnionSerde(union, false, supplier);

        assertThatThrownBy(() -> serde.serializer().serialize(TOPIC, "notByte"))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Can not serialize value as union alternative");
    }

    @Test
    @DisplayName("deserializer: null/empty -> DataNull, tries in order and returns first compatible")
    void deserializeNullsAndOrdering() {
        final var union = new UnionType(
                new UnionType.Member(STR_TYPE),
                new UnionType.Member(BYTE_TYPE)
        );
        final SerdeSupplier supplier = (type, isKey) -> {
            if (type.equals(STR_TYPE)) {
                var kafka = Serdes.String();
                return new Serde<>() {
                    @Override
                    public Serializer<Object> serializer() {
                        return (Serializer<Object>) (Serializer<?>) kafka.serializer();
                    }

                    @Override
                    public Deserializer<Object> deserializer() {
                        return (Deserializer<Object>) (Deserializer<?>) kafka.deserializer();
                    }
                };
            }
            if (type.equals(BYTE_TYPE)) return new ByteSerde();
            throw new IllegalArgumentException("unexpected type");
        };
        final var serde = new UnionSerde(union, false, supplier);

        // null/empty -> DataNull.INSTANCE
        assertThat(serde.deserializer().deserialize(TOPIC, null)).isSameAs(DataNull.INSTANCE);
        assertThat(serde.deserializer().deserialize(TOPIC, new byte[]{})).isSameAs(DataNull.INSTANCE);

        // Bytes that could be read by multiple members should yield the first compatible (String first)
        final var bytes = "A".getBytes();
        final var out = serde.deserializer().deserialize(TOPIC, bytes);
        assertThat(out).isInstanceOf(String.class).isEqualTo("A");
    }

    @Test
    @DisplayName("deserializer throws DataException when all members fail to deserialize")
    void deserializeNoMemberSucceedsThrows() {
        // Define a member whose deserializer always throws
        final var throwingType = new SimpleType(Integer.class, "Int");
        final var throwingSerde = new Serde<>() {
            @Override
            public Serializer<Object> serializer() {
                return new Serializer<>() {
                    @Override
                    public byte[] serialize(String topic, Object data) {
                        return null;
                    }
                };
            }

            @Override
            public Deserializer<Object> deserializer() {
                return new Deserializer<>() {
                    @Override
                    public Object deserialize(String topic, byte[] data) {
                        throw new RuntimeException("nope");
                    }
                };
            }
        };

        final var union = new UnionType(new UnionType.Member(throwingType));
        final SerdeSupplier supplier = (type, isKey) -> throwingSerde;
        final var serde = new UnionSerde(union, false, supplier);

        assertThatThrownBy(() -> serde.deserializer().deserialize(TOPIC, "data".getBytes()))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Can not deserialize data as union");
    }
}
