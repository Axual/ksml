package io.axual.ksml.type;

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


import io.axual.ksml.data.type.DataType;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public record UserType(String notation, DataType dataType) {
    public static final String DEFAULT_NOTATION = "default";
    public static final UserType UNKNOWN = new UserType(DataType.UNKNOWN);

    public UserType(DataType dataType) {
        this(DEFAULT_NOTATION, dataType);
    }

    @NotNull
    @Override
    public String toString() {
        final var notationName = notation != null ? notation.toLowerCase() : "";
        final var schemaName = dataType != null ? dataType.name() : "";
        if (notationName.isEmpty() && schemaName.isEmpty()) return "<unknown type>";
        if (notationName.isEmpty()) return schemaName;
        if (schemaName.isEmpty()) return notationName;
        return notationName + ":" + schemaName;
    }

    public static DataType[] userTypesToDataTypes(UserType[] types) {
        return Arrays.stream(types).map(UserType::dataType).toArray(DataType[]::new);
    }
}
