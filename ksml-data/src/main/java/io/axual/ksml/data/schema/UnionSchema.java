package io.axual.ksml.data.schema;

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

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

@Getter
@EqualsAndHashCode
public class UnionSchema extends DataSchema {
    // Normally a union would contain value schema only, but for PROTOBUF we need to remember field name and index.
    // Therefore, we keep an array of DataFields instead of DataSchemas.
    private final DataField[] valueTypes;

    public UnionSchema(DataField... valueTypes) {
        this(true, valueTypes);
    }

    // Optimize
    // Make this public when the need arises to manually control optimization
    private UnionSchema(boolean flatten, DataField... valueTypes) {
        super(Type.UNION);
        this.valueTypes = flatten ? recursivelyGetValueTypes(valueTypes) : valueTypes;
    }

    private DataField[] recursivelyGetValueTypes(DataField[] valueTypes) {
        // Here we flatten the list of value types by recursively walking through all value types. Any sub-unions
        // are exploded and taken up in this union's list of value types.
        final var result = new ArrayList<DataField>();
        for (final var valueType : valueTypes) {
            if (valueType.schema() instanceof UnionSchema unionSchema) {
                final var subFields = recursivelyGetValueTypes(unionSchema.valueTypes);
                result.addAll(Arrays.stream(subFields).toList());
            } else {
                result.add(valueType);
            }
        }
        return result.toArray(new DataField[]{});
    }

    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        // Don't call the super method here, since that gives wrong semantics. As a union we are
        // assignable from any schema type, so we must skip the comparison of our own schema type
        // with that of the other schema.

        // By convention, we are not assignable if the other schema is null.
        if (otherSchema == null) return false;

        // If the other schema is a union, then we compare all value types of that union.
        if (otherSchema instanceof UnionSchema otherUnionSchema) {
            // This schema is assignable from the other union fields when all of its value types can be assigned to
            // this union.
            for (final var otherUnionsValueType : otherUnionSchema.valueTypes) {
                if (!isAssignableFrom(otherUnionsValueType)) return false;
            }
            return true;
        }

        // The other schema is not a union --> we are assignable from the other schema if at least
        // one of our value schema is assignable from the other schema.
        for (final var valueType : valueTypes) {
            if (valueType.schema().isAssignableFrom(otherSchema)) return true;
        }
        return false;
    }

    private boolean isAssignableFrom(DataField otherField) {
        for (final var valueType : valueTypes) {
            // First check if the schema of this field and the other field are compatible
            if (valueType.schema().isAssignableFrom(otherField.schema())) {
                // If they are, then manually check if we allow assignment from the other field to this field
                if (allowAssignment(valueType, otherField)) return true;
            }
        }
        return false;
    }

    private boolean allowAssignment(DataField thisField, DataField otherField) {
        // Allow assignments from an anonymous union type, having name or index unset
        if (thisField.name() == null || otherField.name() == null) return true;
        if (thisField.index() == DataField.NO_INDEX || otherField.index() == DataField.NO_INDEX) return true;
        // This code is specifically made for PROTOBUF oneOf types, containing a field name and index. We allow
        // assignment only if both fields match.
        if (!Objects.equals(thisField.name(), otherField.name())) return false;
        return Objects.equals(thisField.index(), otherField.index());
    }
}
