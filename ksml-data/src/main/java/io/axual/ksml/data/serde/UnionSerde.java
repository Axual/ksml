package io.axual.ksml.data.serde;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UnionType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serde for KSML union types. It maintains a list of member Serdes and tries them
 * in order for serialization and deserialization, accepting the first compatible
 * type. Null values are always supported and mapped to DataNull for deserialization.
 */
public class UnionSerde implements Serde<Object> {
    private record MemberSerde(DataType type, Serializer<Object> serializer, Deserializer<Object> deserializer) {
    }

    private final List<MemberSerde> memberSerdes = new ArrayList<>();

    /**
     * Constructs a UnionSerde from a KSML UnionType. For each member type, a delegate Serde is
     * obtained from the provided SerdeSupplier.
     *
     * @param unionType     the union type definition
     * @param isKey         whether the resulting Serde will be used for keys
     * @param serdeSupplier supplier used to obtain member Serdes
     */
    public UnionSerde(UnionType unionType, boolean isKey, SerdeSupplier serdeSupplier) {
        for (int index = 0; index < unionType.members().length; index++) {
            final var member = unionType.members()[index];
            try (final var serde = serdeSupplier.get(member.type(), isKey)) {
                memberSerdes.add(new MemberSerde(member.type(), serde.serializer(), serde.deserializer()));
            }
        }
    }

    /**
     * Configures all member serializers and deserializers with the provided configuration.
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        for (final var memberSerde : memberSerdes) {
            memberSerde.serializer.configure(configs, isKey);
            memberSerde.deserializer.configure(configs, isKey);
        }
    }

    @Override
    public Serializer<Object> serializer() {
        return new UnionSerializer();
    }

    @Override
    public Deserializer<Object> deserializer() {
        return new UnionDeserializer();
    }

    // This serializer walks through all its value types and checks if there is a serializer
    // that understands the given input dataType. If so, then it returns the serialized bytes using
    // that serializer. If not, then it throws a runtime exception.
    private class UnionSerializer implements Serializer<Object> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            for (final var memberSerde : memberSerdes) {
                memberSerde.serializer.configure(configs, isKey);
            }
        }

        @Nullable
        @Override
        public byte[] serialize(String topic, Object data) {
            // Always allow null values for unions, so check these first outside of the union's memberTypes
            if (data == null || data == DataNull.INSTANCE) return null;

            // Iterate over all value types and call a type-compatible serializer
            for (final var memberSerde : memberSerdes) {
                // Check if we are serializing a DataObject. If so, then check compatibility using its own data
                // dataType, else check compatibility with Java native dataType.
                if (data instanceof DataObject dataObject) {
                    if (memberSerde.type.isAssignableFrom(dataObject).isAssignable()) {
                        return memberSerde.serializer.serialize(topic, dataObject);
                    }
                } else {
                    if (memberSerde.type.isAssignableFrom(data).isAssignable()) {
                        return memberSerde.serializer.serialize(topic, data);
                    }
                }
            }

            // No type compatibility found, so raise an exception
            throw new DataException("Can not serialize value as union alternative: data=" + data + ", valuesTypes=" + memberTypesToString());
        }
    }

    private class UnionDeserializer implements Deserializer<Object> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            for (final var memberSerde : memberSerdes) {
                memberSerde.deserializer.configure(configs, isKey);
            }
        }

        @Override
        public Object deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return DataNull.INSTANCE;
            }

            for (final var memberSerde : memberSerdes) {
                try {
                    Object result = memberSerde.deserializer.deserialize(topic, data);
                    if (result instanceof DataObject dataObject && memberSerde.type.isAssignableFrom(dataObject).isAssignable())
                        return result;
                    if (memberSerde.type.isAssignableFrom(result).isAssignable()) return result;
                } catch (Exception e) {
                    // Not properly deserialized, so ignore and try the next alternative
                }
            }
            throw new DataException("Can not deserialize data as union: memberTypes=" + memberTypesToString());
        }
    }

    private List<DataType> memberTypesToString() {
        return memberSerdes.stream().map(serde -> serde.type).toList();
    }
}
