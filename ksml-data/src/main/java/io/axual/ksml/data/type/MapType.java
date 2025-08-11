package io.axual.ksml.data.type;

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


import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchemaConstants;

import java.util.Map;

/**
 * A {@link ComplexType} representing a map with String keys and a configurable value type.
 * <p>
 * The map type follows {@link io.axual.ksml.data.schema.DataSchemaConstants#MAP_TYPE} semantics
 * where keys are strings and values are typed according to the provided value {@link DataType}.
 */
public class MapType extends ComplexType {
    public MapType() {
        this(DataType.UNKNOWN);
    }

    public MapType(DataType valueType) {
        super(Map.class,
                buildName("Map", valueType),
                DataSchemaConstants.MAP_TYPE + "(" + buildSpec(valueType) + ")",
                DataString.DATATYPE,
                valueType);
    }

    public DataType keyType() {
        return subType(0);
    }

    public DataType valueType() {
        return subType(1);
    }
}
