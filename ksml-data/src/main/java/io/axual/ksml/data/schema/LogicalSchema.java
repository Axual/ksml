package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import io.axual.ksml.data.schema.logical.DecimalLogicalType;
import io.axual.ksml.data.schema.logical.LogicalType;
import io.axual.ksml.data.util.EqualUtil;
import lombok.EqualsAndHashCode;

import java.util.Objects;

import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/** A base primitive with a {@link LogicalType} attached, Avro-style: decimal on bytes, uuid on string. */
@EqualsAndHashCode(callSuper = true)
public final class LogicalSchema extends DataSchema {
    private static final String NOT_ASSIGNABLE_FROM = " is not assignable from ";

    private final LogicalType logicalType;

    public LogicalSchema(LogicalType logicalType) {
        // Checked inside the super() argument so a null gives the named message instead of a bare NPE.
        super(requireLogicalType(logicalType).baseSchema().type());
        this.logicalType = logicalType;
    }

    private static LogicalType requireLogicalType(LogicalType logicalType) {
        return Objects.requireNonNull(logicalType, "logicalType");
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
        if (!(otherSchema instanceof LogicalSchema other))
            return Assignable.notAssignable(logicalType.name() + NOT_ASSIGNABLE_FROM + otherSchema);
        // A decimal may grow its precision at the same scale, the safe direction for schema evolution.
        // Narrowing the precision or changing the scale can lose digits, so both stay rejected.
        if (logicalType instanceof DecimalLogicalType self && other.logicalType instanceof DecimalLogicalType from) {
            if (self.scale() != from.scale() || self.precision() < from.precision())
                return Assignable.notAssignable(self + NOT_ASSIGNABLE_FROM + from);
            return baseSchema().isAssignableFrom(other.baseSchema());
        }
        if (!logicalType.equals(other.logicalType))
            return Assignable.notAssignable(logicalType.name() + NOT_ASSIGNABLE_FROM + otherSchema);
        return baseSchema().isAssignableFrom(other.baseSchema());
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
    public String toString() {
        return logicalType.name();
    }
}
