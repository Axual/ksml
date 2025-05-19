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

/**
 * An abstract base class for schemas with a name and namespace in the KSML framework.
 * <p>
 * The {@code NamedSchema} class extends {@link DataSchema} and provides a foundation for schemas
 * that require identification through a name and an optional namespace. This class also supports
 * optional documentation for detailed descriptions of the schema.
 * </p>
 */
@Getter
@EqualsAndHashCode
public abstract class NamedSchema extends DataSchema {
    /**
     * The namespace associated with this schema.
     * <p>
     * The namespace is used to group related schemas and avoid naming collisions.
     * This value is optional and may be null if no namespace is used.
     * </p>
     */
    private final String namespace;
    /**
     * The name of this schema.
     * <p>
     * The name serves as a unique identifier for the schema within its namespace.
     * It is required and cannot be null or empty.
     * </p>
     */
    private final String name;
    /**
     * Optional documentation or description for this schema.
     * <p>
     * This value may provide additional context or metadata about the schema's purpose.
     * </p>
     */
    private final String doc;

    /**
     * Constructs a {@code NamedSchema} with the specified type, namespace, name, and documentation.
     *
     * @param type      The type of the schema.
     * @param namespace The namespace associated with this schema. It may be {@code null} if
     *                  no namespace is needed.
     * @param name      The name of this schema. If {@code null} a name will be generated.
     * @param doc       Optional documentation or description for the schema. May be {@code null}.
     * @throws IllegalArgumentException if the {@code name} is null or empty.
     */
    protected NamedSchema(String type, String namespace, String name, String doc) {
        super(type);
        this.namespace = namespace;
        if (name == null || name.isEmpty()) {
            name = "Anonymous" + getClass().getSimpleName();
        }
        this.name = name;
        this.doc = doc;
    }

    /**
     * Checks whether this schema has documentation.
     *
     * @return {@code true} if the {@code doc} field is not {@code null} and not empty;
     * {@code false} otherwise.
     */
    public boolean hasDoc() {
        return doc != null && !doc.isEmpty();
    }

    /**
     * Returns the full name of the schema, combining the namespace and name.
     * <p>
     * If a namespace is defined, the full name is formatted as {@code namespace.name}.
     * If no namespace is defined, the name is returned as is.
     * </p>
     *
     * @return A string representing the fully qualified name of the schema.
     */
    public String fullName() {
        return (namespace != null && !namespace.isEmpty() ? namespace + "." : "") + name;
    }

    /**
     * Determines whether this schema can be assigned from another schema.
     * <p>
     * This method should be implemented by subclasses to define assignability rules
     * specific to their schema implementation.
     * </p>
     *
     * @param otherSchema The other {@link DataSchema} to check for assignability.
     * @return {@code true} if the other schema is assignable to this schema, {@code false} otherwise.
     */
    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        // Return true if the other schema is also a named schema. We purposefully ignore namespace, name and doc
        // fields to make JSON, Protobuf, XML etc comparable schema.
        return otherSchema instanceof NamedSchema;
    }

    /**
     * Returns the string representation of the schema.
     * <p>
     * By default, this is the schema's fully qualified name.
     * </p>
     *
     * @return A string representation of the schema.
     */
    @Override
    public String toString() {
        return fullName();
    }
}
