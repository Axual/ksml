package io.axual.ksml.data.schema;

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

public abstract class NamedSchema extends DataSchema {
    private final String namespace;
    private final String name;
    private final String doc;

    protected NamedSchema(Type type, String namespace, String name, String doc) {
        super(type);
        this.namespace = namespace;
        if (name == null || name.length() == 0) {
            name = "Unnamed" + getClass().getSimpleName();
        }
        this.name = name;
        this.doc = doc;
    }

    public String toString() {
        return (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + name;
    }

    public String namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    public String doc() {
        return doc;
    }

    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof NamedSchema otherNamedSchema)) return false;
        // Return true if the other named schema contains the same metadata
        return (Objects.equals(namespace, otherNamedSchema.namespace)
                && Objects.equals(name, otherNamedSchema.name));
//                && Objects.equals(doc, otherNamedSchema.doc));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (!super.equals(other)) return false;

        // Compare all schema relevant fields
        if (!Objects.equals(namespace, ((NamedSchema) other).namespace)) return false;
        if (!Objects.equals(name, ((NamedSchema) other).name)) return false;
        return Objects.equals(doc, ((NamedSchema) other).doc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), namespace, name, doc);
    }
}
