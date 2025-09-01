package io.axual.ksml.generator;

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


import io.axual.ksml.data.mapper.DataTypeFlattener;
import io.axual.ksml.data.serde.UnionSerde;
import io.axual.ksml.data.type.UnionType;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.serialization.Serde;
import org.jetbrains.annotations.NotNull;

public record StreamDataType(UserType userType, boolean isKey) {
    private static final DataTypeFlattener FLATTENER = new DataTypeFlattener();

    public boolean isAssignableFrom(StreamDataType other) {
        return userType.dataType().isAssignableFrom(other.userType.dataType());
    }

    public StreamDataType flatten() {
        return isKey ? FLATTENER.flatten(this) : this;
    }

    @NotNull
    @Override
    public String toString() {
        var schemaName = userType.dataType().name();
        return (userType.notation() != null ? userType.notation().toLowerCase() : "unknown notation") + (schemaName != null && !schemaName.isEmpty() ? ":" : "") + schemaName;
    }

    public Serde<Object> serde() {
        final var notation = ExecutionContext.INSTANCE.notationLibrary().get(userType.notation());
        if (userType.dataType() instanceof UnionType unionType)
            return new UnionSerde(FLATTENER.flatten(unionType), isKey, notation::serde);
        var serde = notation.serde(FLATTENER.flatten(userType.dataType()), isKey);
        return ExecutionContext.INSTANCE.wrapSerde(serde);
    }
}
