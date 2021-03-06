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

public class UserKeyValueListType extends UserListType {
    public UserKeyValueListType(String notation, UserType keyType, UserType valueType) {
        super(notation, new UserKeyValueType(notation, keyType, valueType));
    }

    public UserType keyValueKeyType() {
        return ((UserKeyValueType) valueType()).keyType();
    }

    public UserType keyValueValueType() {
        return ((UserKeyValueType) valueType()).valueType();
    }

    public static UserKeyValueListType createFrom(UserType type) {
        if (type instanceof UserListType && ((UserListType) type).valueType() instanceof UserTupleType) {
            UserTupleType valueType = (UserTupleType) ((UserListType) type).valueType();
            if (valueType.subTypeCount() == 2) {
                return new UserKeyValueListType(type.notation(), valueType.subType(0), valueType.subType(1));
            }
        }
        throw new KSMLTopologyException("Could not convert type to KeyValueList: " + type);
    }
}
