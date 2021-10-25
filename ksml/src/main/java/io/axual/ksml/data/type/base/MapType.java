package io.axual.ksml.data.type.base;

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


import java.util.Map;

import io.axual.ksml.schema.DataSchema;

public class MapType extends ComplexType {
    private final DataSchema schema;

    public MapType(DataType keyType, DataType valueType, DataSchema schema) {
        super(Map.class, keyType, valueType);
        this.schema = schema;
    }

    public DataType keyType() {
        return subType(0);
    }

    public DataType valueType() {
        return subType(1);
    }

    public DataSchema schema() {
        return schema;
    }

    @Override
    public String schemaName() {
        if (schema != null) {
            return schema.name();
        }
        return schemaName("Map", "To");
    }
}
