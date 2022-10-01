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

import java.util.Objects;

public class MapSchema extends DataSchema {
    private final DataSchema valueSchema;

    public MapSchema(DataSchema valueSchema) {
        super(Type.MAP);
        this.valueSchema = valueSchema;
    }

    public DataSchema valueSchema() {
        return valueSchema;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields, note: explicitly do not compare the doc field
        return (Objects.equals(valueSchema, ((MapSchema) other).valueSchema()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valueSchema);
    }
}
