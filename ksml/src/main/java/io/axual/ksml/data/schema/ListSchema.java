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

import java.util.Objects;

public class ListSchema extends DataSchema {
    private final DataSchema valueSchema;

    public ListSchema(DataSchema valueSchema) {
        super(Type.LIST);
        this.valueSchema = valueSchema;
    }

    public DataSchema valueSchema() {
        return valueSchema;
    }

    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof ListSchema otherListSchema)) return false;
        // If the value schema is null, then any schema can be assigned.
        if (valueSchema == null) return true;
        // This schema is assignable from the other schema when the value schema is assignable from
        // the otherSchema's value schema.
        return valueSchema.isAssignableFrom(otherListSchema.valueSchema);
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields
        return (Objects.equals(valueSchema, ((ListSchema) other).valueSchema));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valueSchema);
    }
}
