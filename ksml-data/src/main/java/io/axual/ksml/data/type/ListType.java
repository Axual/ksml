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

import java.util.List;

/**
 * A {@link ComplexType} representing a list/array type with a single element type.
 */
public class ListType extends ComplexType {
    public ListType() {
        this(DataType.UNKNOWN);
    }

    public ListType(DataType valueType) {
        super(List.class, buildName("List", valueType), "[" + buildSpec(valueType) + "]", valueType);
    }

    public DataType valueType() {
        return subType(0);
    }

    public static ListType createFrom(DataType type) {
        if (type instanceof ComplexType outerType &&
                List.class.isAssignableFrom(outerType.containerClass()) &&
                outerType.subTypeCount() == 1) {
            return new ListType(outerType.subType(0));
        }
        return null;
    }
}
