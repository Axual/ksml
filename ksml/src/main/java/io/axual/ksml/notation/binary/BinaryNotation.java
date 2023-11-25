package io.axual.ksml.notation.binary;

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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.value.Null;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.serde.NullDeserializer;
import io.axual.ksml.serde.NullSerializer;
import io.axual.ksml.util.DataUtil;

public class BinaryNotation implements Notation {
    public static final String NOTATION_NAME = "BINARY";
    private static final NativeDataObjectMapper mapper = new NativeDataObjectMapper();
    private final Notation jsonNotation = new JsonNotation();

    @Override
    public String name() {
        return NOTATION_NAME;
    }

    @Override
    public Serde<Object> getSerde(DataType type, boolean isKey) {
        if (type instanceof SimpleType) {
            if (type.containerClass() == Null.class)
                return new BinarySerde(Serdes.serdeFrom(new NullSerializer(), new NullDeserializer()));
            return new BinarySerde((Serde<Object>) Serdes.serdeFrom(type.containerClass()));
        }
        // If not a simple dataType, then rely on JSON encoding
        return jsonNotation.getSerde(type, isKey);
    }

    private static class BinarySerde implements Serde<Object> {
        private final Serializer<Object> serializer;
        private final Deserializer<Object> deserializer;

        public BinarySerde(Serde<Object> serde) {
            this.serializer = serde.serializer();
            this.deserializer = serde.deserializer();
        }

        private final Serializer<Object> wrapSerializer = new Serializer<>() {
            @Override
            public byte[] serialize(String topic, Object data) {
                // Serialize the raw object by converting from user object if necessary
                return serializer.serialize(topic, mapper.fromDataObject(DataUtil.asDataObject(data)));
            }
        };

        private final Deserializer<Object> wrapDeserializer = new Deserializer<>() {
            @Override
            public Object deserialize(String topic, byte[] data) {
                // Deserialize the raw object and return as such. If any conversion to a user
                // object needs to be done, then it's up to the pipeline operations to do so.
                // This ensures that operations that need the raw type (eg. Count needing Long)
                // can read back the binary types they expect.
                return deserializer.deserialize(topic, data);
            }
        };

        @Override
        public Serializer<Object> serializer() {
            return wrapSerializer;
        }

        @Override
        public Deserializer<Object> deserializer() {
            return wrapDeserializer;
        }
    }
}
