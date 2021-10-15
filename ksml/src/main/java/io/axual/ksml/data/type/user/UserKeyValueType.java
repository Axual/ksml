package io.axual.ksml.data.type.user;

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


import io.axual.ksml.exception.KSMLTopologyException;

public class UserKeyValueType extends UserTupleType implements UserType {
    public UserKeyValueType(String notation, UserType keyType, UserType valueType) {
        super(notation, keyType, valueType);
    }

    public UserType keyType() {
        return subType(0);
    }

    public UserType valueType() {
        return subType(1);
    }

    @Override
    public String schemaName() {
        return schemaName("KeyValue");
    }

    public static UserKeyValueType createFrom(UserType type) {
        if (type instanceof UserTupleType && ((UserTupleType) type).subTypeCount() == 2) {
            return new UserKeyValueType(type.notation(), ((UserTupleType) type).subType(0), ((UserTupleType) type).subType(1));
        }
        throw new KSMLTopologyException("Could not convert type to KeyValue: " + type);
    }
}
