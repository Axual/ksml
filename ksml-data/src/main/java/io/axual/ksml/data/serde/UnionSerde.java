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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UnionType;

public class UnionSerde implements Serde<Object> {
    private record PossibleType(DataType type, Serializer<Object> serializer,
                                Deserializer<Object> deserializer) {
    }

    private final List<PossibleType> possibleTypes = new ArrayList<>();

    public UnionSerde(UnionType type, boolean isKey, SerdeSupplier serdeSupplier) {
        for (int index = 0; index < type.possibleTypes().length; index++) {
            var possibleType = type.possibleTypes()[index];
            try (final var serde = serdeSupplier.get(possibleType, isKey)) {
                possibleTypes.add(new PossibleType(possibleType, serde.serializer(), serde.deserializer()));
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        for (PossibleType possibleType : possibleTypes) {
            possibleType.serializer.configure(configs, isKey);
            possibleType.deserializer.configure(configs, isKey);
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

    // This serializer walks through all its possible types and checks if there is a serializer
    // that understands the given input dataType. If so, then it returns the serialized bytes using
    // that serializer. If not, then it throws a runtime exception.
    private class UnionSerializer implements Serializer<Object> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            for (PossibleType possibleType : possibleTypes) {
                possibleType.serializer.configure(configs, isKey);
            }
        }

        @Override
        public byte[] serialize(String topic, Object data) {
            for (PossibleType possibleType : possibleTypes) {
                // Check if we are serializing a DataObject. If so, then check compatibility
                // using its own data dataType, else check compatibility with Java native dataType.
                if (data instanceof DataObject dataObject) {
                    if (possibleType.type.isAssignableFrom(dataObject)) {
                        return possibleType.serializer.serialize(topic, dataObject);
                    }
                } else {
                    if (possibleType.type.isAssignableFrom(data)) {
                        return possibleType.serializer.serialize(topic, data);
                    }
                }
            }
            throw new ExecutionException("Can not serialize object as union alternative: " + data.getClass().getSimpleName());
        }
    }

    private class UnionDeserializer implements Deserializer<Object> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            for (PossibleType possibleType : possibleTypes) {
                possibleType.deserializer.configure(configs, isKey);
            }
        }

        @Override
        public Object deserialize(String topic, byte[] data) {
            if (data == null || data.length == 0) {
                return DataNull.INSTANCE;
            }

            for (PossibleType possibleType : possibleTypes) {
                try {
                    Object result = possibleType.deserializer.deserialize(topic, data);
                    if (result instanceof DataObject dataObject && possibleType.type.isAssignableFrom(dataObject))
                        return result;
                    if (possibleType.type.isAssignableFrom(result)) return result;
                } catch (Exception e) {
                    // Not properly deserialized, so ignore and try next alternative
                }
            }
            throw new ExecutionException("Can not deserialize data as union possible dataType" + possibleTypes);
        }
    }
}
