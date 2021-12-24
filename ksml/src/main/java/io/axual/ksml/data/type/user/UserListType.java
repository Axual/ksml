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


import java.util.List;

import io.axual.ksml.data.type.base.ListType;
import io.axual.ksml.exception.KSMLTopologyException;

public class UserListType extends ComplexUserType {
    public UserListType(String notation, UserType valueType) {
        super(new ListType(valueType.type()), notation, valueType);
    }

    @Override
    public String schemaName() {
        return "ListOf" + valueType().schemaName();
    }

    public UserType valueType() {
        return subType(0);
    }

    public static UserListType createFrom(UserType type) {
        if (type instanceof ComplexUserType) {
            ComplexUserType outerType = (ComplexUserType) type;
            if (List.class.isAssignableFrom(outerType.type().containerClass()) && outerType.subTypeCount() == 1) {
                return new UserListType(type.notation(), outerType.subType(0));
            }
        }
        throw new KSMLTopologyException("Could not convert type to List: " + type);
    }
}
