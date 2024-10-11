package io.axual.ksml.data.notation.binary;

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

import io.axual.ksml.data.loader.SchemaLoader;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.Notation;
import io.axual.ksml.data.notation.NotationConverter;
import io.axual.ksml.data.serde.ByteSerde;
import io.axual.ksml.data.serde.NullSerde;
import io.axual.ksml.data.serde.SerdeSupplier;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.SimpleType;
import io.axual.ksml.data.value.Null;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class BinaryNotation implements Notation {
    public static final String NAME = "binary";
    public static final DataType DEFAULT_TYPE = DataType.UNKNOWN;
    private final NativeDataObjectMapper nativeMapper;
    private final SerdeSupplier complexTypeSerdeSupplier;
    @Getter
    private final NotationConverter converter = null;
    @Getter
    private final SchemaLoader loader = null;

    public BinaryNotation(NativeDataObjectMapper nativeMapper, SerdeSupplier complexTypeSerdeSupplier) {
        this.nativeMapper = nativeMapper;
        this.complexTypeSerdeSupplier = complexTypeSerdeSupplier;
    }

    @Override
    public DataType defaultType() {
        return DEFAULT_TYPE;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        if (type instanceof SimpleType) {
            if (type.containerClass() == Null.class) return new NullSerde();
            if (type.containerClass() == Byte.class) return new ByteSerde();
            return new BinarySerde((Serde<Object>) Serdes.serdeFrom(type.containerClass()));
        }
        // If not a simple dataType, then rely on JSON encoding
        return complexTypeSerdeSupplier.get(type, isKey);
    }

    @Getter
    private class BinarySerde implements Serde<Object> {
        private final Serializer<Object> serializer;
        private final Deserializer<Object> deserializer;

        public BinarySerde(final Serde<Object> serde) {
            this.serializer = (topic, data) -> {
                // Serialize the raw object by converting from user object if necessary
                return serde.serializer().serialize(topic, nativeMapper.fromDataObject(nativeMapper.toDataObject(data)));
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
