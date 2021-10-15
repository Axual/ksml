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


import io.axual.ksml.data.type.base.MapType;
import io.axual.ksml.schema.DataSchema;

public class UserMapType extends ComplexUserType {
    public UserMapType(String notation, UserType keyType, UserType valueType, DataSchema schema) {
        super(new MapType(keyType.type(), valueType.type(), schema), notation, keyType, valueType);
    }

    public UserType keyType() {
        return subType(0);
    }

    public UserType valueType() {
        return subType(1);
    }

    @Override
    public String schemaName() {
        return schemaName("Map", "To");
    }
}
