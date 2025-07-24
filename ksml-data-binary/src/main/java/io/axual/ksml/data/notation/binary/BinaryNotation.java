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

import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.base.BaseNotation;
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

public class BinaryNotation extends BaseNotation {
    public static final String NOTATION_NAME = "binary";
    public static final DataType DEFAULT_TYPE = DataType.UNKNOWN;
    private final SerdeSupplier complexTypeSerdeSupplier;

    public BinaryNotation(NotationContext context, SerdeSupplier complexTypeSerdeSupplier) {
        super(context, null, DEFAULT_TYPE, null, null);
        this.complexTypeSerdeSupplier = complexTypeSerdeSupplier;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // If the data type is a simple type, then handle here
        if (type instanceof SimpleType) {
            if (type.containerClass() == Null.class) return new NullSerde();
            if (type.containerClass() == Byte.class) return new ByteSerde();
            return new BinarySerde((Serde<Object>) Serdes.serdeFrom(type.containerClass()));
        }

        // If not a simple data type, then return an (externally supplied) serde
        if (complexTypeSerdeSupplier != null) return complexTypeSerdeSupplier.get(type, isKey);

        // No serde found
        throw noSerdeFor(type);
    }

    @Getter
    private class BinarySerde implements Serde<Object> {
        private final Serializer<Object> serializer;
        private final Deserializer<Object> deserializer;

        public BinarySerde(final Serde<Object> serde) {
            // Serialize the raw object by converting from a data object if necessary
            this.serializer = (topic, data) -> serde.serializer().serialize(topic, context().nativeDataObjectMapper().fromDataObject(context().nativeDataObjectMapper().toDataObject(data)));
            // Deserialize the raw object and return as such. If any conversion to a user
            // object needs to be done, then it's up to the pipeline operations to do so.
            // This ensures that operations that need the raw type (e.g., Count needing Long)
            // can read back the binary types they expect.
            this.deserializer = (topic, data) -> serde.deserializer().deserialize(topic, data);
        }
    }
}
