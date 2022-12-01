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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.schema.mapper.WindowedSchemaMapper;
import io.axual.ksml.serde.UnionSerde;

public record StreamDataType(NotationLibrary notationLibrary, UserType userType, boolean isKey) {
    public StreamDataType(NotationLibrary notationLibrary, UserType userType, boolean isKey) {
        this.notationLibrary = notationLibrary;
        this.userType = new UserType(userType.notation(), cookType(userType.dataType()));
        this.isKey = isKey;
    }

    private static DataType cookType(DataType type) {
        // When we get a WindowedType, we automatically convert it into a Struct dataType using
        // fixed fields. This allows for processing downstream, since the WindowType itself
        // is KafkaStreams internal and thus not usable in user functions.
        return type instanceof WindowedType windowedType
                ? new StructType(new WindowedSchemaMapper().toDataSchema(windowedType))
                : type;
    }

    private static UnionType cookUnion(UnionType type) {
        var cookedUnionTypes = new UserType[type.possibleTypes().length];
        for (int index = 0; index < type.possibleTypes().length; index++) {
            var userType = type.possibleTypes()[index];
            cookedUnionTypes[index] = new UserType(userType.notation(), cookType(userType.dataType()));
        }
        return new UnionType(cookedUnionTypes);
    }

    public boolean isAssignableFrom(StreamDataType other) {
        return userType.dataType().isAssignableFrom(other.userType.dataType());
    }

    @Override
    public String toString() {
        var schemaName = userType.dataType().schemaName();
        return (userType.notation() != null ? userType.notation().toLowerCase() : "unknown notation") + (schemaName != null && !schemaName.isEmpty() ? ":" : "") + schemaName;
    }

    public Serde<Object> getSerde() {
        if (userType.dataType() instanceof UnionType unionType)
            return new UnionSerde(notationLibrary, cookUnion(unionType), isKey);
        return notationLibrary.get(userType.notation()).getSerde(userType.dataType(), isKey);
    }
}
