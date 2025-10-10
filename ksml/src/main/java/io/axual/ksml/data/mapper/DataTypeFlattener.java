package io.axual.ksml.data.mapper;

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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.dsl.WindowedSchema;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.type.UserType;

import static io.axual.ksml.dsl.WindowedSchema.generateWindowedSchema;

public class DataTypeFlattener extends DataTypeDataSchemaMapper {
    @Override
    public DataSchema toDataSchema(String namespace, String name, DataType type) {
        // Check for the Kafka Streams / KSML specific types here, otherwise return default conversion
        if (type instanceof WindowedType windowedType)
            return WindowedSchema.generateWindowedSchema(windowedType, this::toDataSchema);
        return super.toDataSchema(namespace, name, type);
    }

    public StreamDataType flatten(StreamDataType streamDataType) {
        return new StreamDataType(flatten(streamDataType.userType()), streamDataType.isKey());
    }

    public UserType flatten(UserType userType) {
        return new UserType(userType.notation(), flatten(userType.dataType()));
    }

    public DataType flatten(DataType dataType) {
        // This method translates any internal Kafka Streams datatype to an externally usable type

        // When we get a WindowedType, we automatically convert it into a Struct dataType using
        // fixed fields. This allows for processing downstream, since the WindowType itself
        // is KafkaStreams internal and thus not usable in user functions.
        if (dataType instanceof WindowedType windowedType) {
            return new StructType(generateWindowedSchema(windowedType, this::toDataSchema));
        }

        // No conversion is necessary, so just return the type as is
        return dataType;
    }

    public UnionType flatten(UnionType unionType) {
        // When we get a UnionType, we flatten it recursively
        var flatMembers = new UnionType.Member[unionType.members().length];
        for (int index = 0; index < unionType.members().length; index++) {
            flatMembers[index] = flatten(unionType.members()[index]);
        }
        return new UnionType(flatMembers);
    }

    private UnionType.Member flatten(UnionType.Member fieldType) {
        return new UnionType.Member(fieldType.name(), flatten(fieldType.type()), fieldType.tag());
    }
}
