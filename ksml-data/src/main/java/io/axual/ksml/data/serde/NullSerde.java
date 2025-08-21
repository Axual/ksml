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
import io.axual.ksml.data.value.Null;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serde for representing a KSML Data Null value over Kafka.
 * <p>
 * Serialization always produces a null byte array. Deserialization accepts null or empty
 * byte arrays and returns the KSML {@link io.axual.ksml.data.value.Null#NULL} sentinel. Any
 * non-empty input will result in a DataException.
 */
@Getter
public class NullSerde implements Serde<Object> {
    private static final byte[] SERIALIZED_NULL = null;
    private final Serializer<Object> serializer = (topic, data) -> SERIALIZED_NULL;
    private final Deserializer<Object> deserializer = (topic, data) -> {
        if (data == null || data.length == 0) return Null.NULL;
        throw new DataException("Can only deserialize empty byte arrays as DataNull");
    };
}
