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

public class FixedSchema extends NamedSchema {
    private final int size;

    public FixedSchema(String namespace, String name, String doc, int size) {
        super(Type.FIXED, namespace, name, doc);
        this.size = size;
    }

    public int size() {
        return size;
    }

    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof FixedSchema otherFixedSchema)) return false;
        // This schema is assignable from the other schema when the maximum size is greater or
        // equal than the other schema's maximum size.
        return size >= otherFixedSchema.size;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        return size == ((FixedSchema) other).size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), size);
    }
}
