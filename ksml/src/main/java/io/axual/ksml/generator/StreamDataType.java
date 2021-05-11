package io.axual.ksml.generator;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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



import org.apache.kafka.common.serialization.Serde;

import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.serde.UnknownTypeSerde;
import io.axual.ksml.type.AvroType;
import io.axual.ksml.type.DataType;
import io.axual.ksml.type.SimpleType;
import io.axual.ksml.type.WindowType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StreamDataType {
    // This static is ugly, but works for now...
    private static SerdeGenerator serdeGenerator;
    public final DataType type;
    public final Serde<Object> serde;

    public boolean isAssignableFrom(StreamDataType other) {
        return type.isAssignableFrom(other.type);
    }

    @Override
    public String toString() {
        return type + " (" + (serde != null ? "with " + serde.getClass().getSimpleName() : "no serde") + ")";
    }

    public static void setSerdeGenerator(SerdeGenerator serdeGenerator) {
        StreamDataType.serdeGenerator = serdeGenerator;
    }

    public static StreamDataType of(DataType type, boolean isKey) {
        if (serdeGenerator == null) {
            throw new KSMLExecutionException("Serde Generator not initialized");
        }
        if (type instanceof WindowType) {
            // For WindowTypes return a serde of the value contained within the window
            return new StreamDataType(type, serdeGenerator.getSerdeForType(((WindowType) type).getWindowedType(), isKey));
        }
        if (type instanceof AvroType) {
            return new StreamDataType(type, serdeGenerator.getSerdeForType(type, isKey));
        }
        if (type instanceof SimpleType) {
            // For simple types return a serde for that particular type
            return new StreamDataType(type, serdeGenerator.getSerdeForType(type, isKey));
        }
        // Return a default serde, which produces exceptions only when (de)serializing
        return new StreamDataType(type, new UnknownTypeSerde(type));
    }
}
