package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.Equality;
import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.schema.logical.LogicalType;
import io.axual.ksml.data.util.EqualUtil;

import java.util.Objects;

import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/**
 * A schema node decorating a base primitive with a {@link LogicalType} (Avro-style: decimal on bytes,
 * uuid on string). It reports the base primitive's {@link #type()} so assignability and logical-unaware
 * notations treat it as the base, and being distinct from the primitive singletons it forces mappers to
 * handle it explicitly.
 */
public final class LogicalSchema extends DataSchema {
    private final LogicalType logicalType;

    public LogicalSchema(LogicalType logicalType) {
        super(logicalType.baseSchema().type());
        this.logicalType = Objects.requireNonNull(logicalType, "logicalType");
    }

    public LogicalType logicalType() {
        return logicalType;
    }

    public DataSchema baseSchema() {
        return logicalType.baseSchema();
    }

    @Override
    public Assignable isAssignableFrom(DataSchema otherSchema) {
        if (otherSchema == null) return Assignable.notAssignable("No other schema provided");
        final var effectiveOther = otherSchema instanceof LogicalSchema other ? other.baseSchema() : otherSchema;
        return baseSchema().isAssignableFrom(effectiveOther);
    }

    @Override
    public Equality equals(Object other, EqualityFlags flags) {
        if (this == other) return Equality.equal();
        if (other == null) return otherIsNull(this);
        if (!getClass().equals(other.getClass()))
            return EqualUtil.containerClassNotEqual(getClass(), other.getClass());

        final var that = (LogicalSchema) other;
        if (!Objects.equals(logicalType, that.logicalType))
            return fieldNotEqual("logicalType", this, logicalType, that, that.logicalType);

        return Equality.equal();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        return other instanceof LogicalSchema that
                && Objects.equals(type(), that.type())
                && Objects.equals(logicalType, that.logicalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type(), logicalType);
    }

    @Override
    public String toString() {
        return logicalType.name();
    }
}
