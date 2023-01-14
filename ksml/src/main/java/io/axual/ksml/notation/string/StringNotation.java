package io.axual.ksml.notation.string;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.Notation;
import io.axual.ksml.util.DataUtil;
import org.apache.kafka.common.serialization.*;

public abstract class StringNotation implements Notation {
    private final StringSerde serde = new StringSerde();
    private final StringMapper<DataObject> mapper;

    public StringNotation(StringMapper<DataObject> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Serde<Object> getSerde(DataType type, boolean isKey) {
        return serde;
    }

    protected RuntimeException noSerdeFor(DataType type) {
        return FatalError.executionError(name() + " serde not found for data type: " + type);
    }

    private class StringSerde implements Serde<Object> {
        private final StringSerializer serializer = new StringSerializer();
        private final StringDeserializer deserializer = new StringDeserializer();

        private final Serializer<Object> wrapSerializer = (topic, data) -> {
            var str = mapper.toString(DataUtil.asDataObject(data));
            return serializer.serialize(topic, str);
        };

        private final Deserializer<Object> wrapDeserializer = (topic, data) -> {
            String str = deserializer.deserialize(topic, data);
            return mapper.fromString(str);
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
