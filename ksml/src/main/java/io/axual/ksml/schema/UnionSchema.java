package io.axual.ksml.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import java.util.Arrays;
import java.util.Objects;

public class UnionSchema extends DataSchema {
    private final DataSchema[] possibleSchemas;

    public UnionSchema(DataSchema... possibleSchemas) {
        super(Type.UNION);
        this.possibleSchemas = possibleSchemas;
    }

    public DataSchema[] possibleSchemas() {
        return possibleSchemas;
    }

    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        // Don't call the super method here, since that gives wrong semantics. As a union we are
        // assignable from any schema type, so we must skip the comparison of our own schema type
        // with that of the other schema.

        // By convention, we are not assignable if the other schema is null.
        if (otherSchema == null) return false;

        // If the other schema is a union, then we compare all possible types of that union.
        if (otherSchema instanceof UnionSchema otherUnionSchema) {
            // This schema is assignable from the other union schema when all of its possible
            // schema can be assigned to this union schema.
            for (DataSchema otherUnionsPossibleSchema : otherUnionSchema.possibleSchemas) {
                if (!isAssignableFrom(otherUnionsPossibleSchema)) return false;
            }
            return true;
        }

        // The other schema is not a union --> we are assignable from the other schema if at least
        // one of our possible schema is assignable from the other schema.
        for (DataSchema possibleSchema : possibleSchemas) {
            if (possibleSchema.isAssignableFrom(otherSchema)) return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (!super.equals(other)) return false;

        // Compare all schema relevant fields
        return Arrays.equals(possibleSchemas, ((UnionSchema) other).possibleSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(possibleSchemas));
    }
}
