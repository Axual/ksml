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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataUnion;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UnionType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UnionSerde implements Serde<Object> {
    private record UnionValueType(DataType type, Serializer<Object> serializer,
                                  Deserializer<Object> deserializer) {
    }

    private final List<UnionValueType> valueTypes = new ArrayList<>();

    public UnionSerde(UnionType unionType, boolean isKey, SerdeSupplier serdeSupplier) {
        for (int index = 0; index < unionType.valueTypes().length; index++) {
            final var valueType = unionType.valueTypes()[index];
            try (final var serde = serdeSupplier.get(valueType.type(), isKey)) {
                valueTypes.add(new UnionValueType(valueType.type(), serde.serializer(), serde.deserializer()));
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        for (final var valueType : valueTypes) {
            valueType.serializer.configure(configs, isKey);
            valueType.deserializer.configure(configs, isKey);
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
            for (final var valueType : valueTypes) {
                valueType.serializer.configure(configs, isKey);
            }
        }

        @Override
        public byte[] serialize(String topic, Object data) {
            // Always allow null values for unions, so check these first outside of the union's valueTypes
            final var value = data instanceof DataUnion dataUnion ? dataUnion.value() : data;
            if (value == null || value == DataNull.INSTANCE) return null;

            // Iterate over all value types and call a type-compatible serializer
            for (final var valueType : valueTypes) {
                // Check if we are serializing a DataObject. If so, then check compatibility
                // using its own data dataType, else check compatibility with Java native dataType.
                if (data instanceof DataObject dataObject) {
                    if (valueType.type.isAssignableFrom(dataObject)) {
                        return valueType.serializer.serialize(topic, dataObject);
                    }
                } else {
                    if (valueType.type.isAssignableFrom(data)) {
                        return valueType.serializer.serialize(topic, data);
                    }
                }
            }

            // No type compatibility found, so raise an exception
            throw new ExecutionException("Can not serialize value as union alternative: value=" + value + ", valuesTypes=" + valueTypesToString());
        }
    }

    private class UnionDeserializer implements Deserializer<Object> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            for (final var valueType : valueTypes) {
                valueType.deserializer.configure(configs, isKey);
            }
        }

        @Override
        public Object deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return DataNull.INSTANCE;
            }

            for (final var valueType : valueTypes) {
                try {
                    Object result = valueType.deserializer.deserialize(topic, data);
                    if (result instanceof DataObject dataObject && valueType.type.isAssignableFrom(dataObject))
                        return result;
                    if (valueType.type.isAssignableFrom(result)) return result;
                } catch (Exception e) {
                    // Not properly deserialized, so ignore and try next alternative
                }
            }
            throw new ExecutionException("Can not deserialize data as union: valueTypes=" + valueTypesToString());
        }
    }

    private List<DataType> valueTypesToString() {
        return valueTypes.stream().map(t -> t.type).toList();
    }
}
