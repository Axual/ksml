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
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class UnknownTypeSerde implements Serde<DataObject> {
    private final DataType type;

    public UnknownTypeSerde() {
        this.type = DataType.UNKNOWN;
    }

    public UnknownTypeSerde(DataType type) {
        this.type = type;
    }

    @Override
    public Serializer<DataObject> serializer() {
        return (topic, object) -> {
            throw new DataException("Can not serialize data dataType \"" + type + "\" from topic " + topic);
        };
    }

    @Override
    public Deserializer<DataObject> deserializer() {
        return (topic, bytes) -> {
            throw new DataException("Can not deserialize data dataType \"" + type + "\" from topic " + topic);
        };
    }
}
