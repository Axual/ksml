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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.value.Null;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.serde.ByteSerde;
import io.axual.ksml.serde.NullSerde;
import io.axual.ksml.util.DataUtil;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class BinaryNotation implements Notation {
    public static final String NOTATION_NAME = "BINARY";
    private static final NativeDataObjectMapper mapper = new NativeDataObjectMapper();
    private final Notation jsonNotation = new JsonNotation();

    @Override
    public String name() {
        return NOTATION_NAME;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (type instanceof SimpleType) {
            if (type.containerClass() == Null.class) return new NullSerde();
            if (type.containerClass() == Byte.class) return new ByteSerde();
            return new BinarySerde((Serde<Object>) Serdes.serdeFrom(type.containerClass()));
        }
        // If not a simple dataType, then rely on JSON encoding
        return jsonNotation.serde(type, isKey);
    }

    @Getter
    private static class BinarySerde implements Serde<Object> {
        private final Serializer<Object> serializer;
        private final Deserializer<Object> deserializer;

        public BinarySerde(final Serde<Object> serde) {
            this.serializer = (topic, data) -> {
                // Serialize the raw object by converting from user object if necessary
                return serde.serializer().serialize(topic, mapper.fromDataObject(DataUtil.asDataObject(data)));
            };
            this.deserializer = (topic, data) -> {
                // Deserialize the raw object and return as such. If any conversion to a user
                // object needs to be done, then it's up to the pipeline operations to do so.
                // This ensures that operations that need the raw type (eg. Count needing Long)
                // can read back the binary types they expect.
                return serde.deserializer().deserialize(topic, data);
            };
        }
    }
}
