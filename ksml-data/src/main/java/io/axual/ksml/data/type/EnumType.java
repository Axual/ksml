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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.util.EqualUtil;
import io.axual.ksml.data.util.ListUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static io.axual.ksml.data.type.DataTypeFlags.IGNORE_ENUM_TYPE_SCHEMA;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/**
 * A {@link SimpleType} representing an enumeration of allowed string symbols.
 */
@Getter
@EqualsAndHashCode
public class EnumType extends SimpleType {
    private final EnumSchema schema;

    public EnumType(EnumSchema schema) {
        super(String.class, schema.name(), DataSchemaConstants.ENUM_TYPE);
        this.schema = schema;
    }

    @Override
    public Assignable isAssignableFrom(DataObject value) {
        final var superAssignable = super.isAssignableFrom(value);
        if (superAssignable.isError()) return superAssignable;
        final var valueStr = value.toString();
        if (ListUtil.find(schema.symbols(), s -> s.name().equals(valueStr)) != null) return Assignable.ok();
        final var symbolsStr = schema.symbols().stream().map(s -> "\"" + s.name() + "\"").toList();
        return Assignable.error("Symbol \"" + valueStr + "\" not found in enumeration with symbols " + String.join(", ", symbolsStr));
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param other The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Equal equals(Object other, Flags flags) {
        if (this == other) return Equal.ok();
        if (other == null) return otherIsNull(this);
        if (!getClass().equals(other.getClass())) return EqualUtil.containerClassNotEqual(getClass(), other.getClass());

        final var superEqual = super.equals(other, flags);
        if (superEqual.isError()) return superEqual;

        final var that = (EnumType) other;

        // Compare schema
        if (!flags.isSet(IGNORE_ENUM_TYPE_SCHEMA) && (schema != null || that.schema != null)) {
            if (schema == null || that.schema == null)
                return fieldNotEqual("schema", this, schema, that, that.schema);
            final var schemaEqual = schema.equals(that.schema, flags);
            if (schemaEqual.isError())
                return fieldNotEqual("schema", this, schema, that, that.schema, schemaEqual);
        }

        return Equal.ok();
    }
}
