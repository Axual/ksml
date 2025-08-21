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

import com.google.common.collect.Lists;

import java.util.*;

import lombok.Builder;
import lombok.Singular;

/**
 * Represents a structured schema with named fields in the KSML framework.
 * <p>
 * The {@code StructSchema} class extends {@link NamedSchema} and is used to define
 * schemas for structured data. A structured schema is composed of a set of named fields,
 * where each field has a name, a type (defined by {@link DataSchema}), and optionally additional metadata.
 * </p>
 */
public class StructSchema extends NamedSchema {
    /**
     * This instance exists for compatibility reasons: if we define eg. JSON Objects, or schema-less Structs, then we
     * need to somehow capture this in a schema with the proper type. The StructSchema is the proper type, so we let
     * the absence of a schema be reflected through null fields. Only 1 instance without a name is allowed, so code
     * that checks if the StructSchema represents "schemaless" can simply perform an equality ('==') check.
     */
    public static final StructSchema SCHEMALESS = new StructSchema(null, null, null, null);
    /**
     * A list of fields that make up the structured schema.
     * <p>
     * Each field is defined by a {@link DataField} object, which includes its name and schema definition.
     * The order of fields in this list is preserved.
     * </p>
     */
    private final List<DataField> fields = new ArrayList<>();
    /**
     * A map of field names to their corresponding {@link DataField} objects.
     * <p>
     * This map provides efficient access to fields by their names.
     * </p>
     */
    private final Map<String, DataField> fieldsByName = new HashMap<>();

    /**
     * Copy constructor for creating a new {@code StructSchema} based on an existing one.
     * <p>
     * The new schema will contain the same fields and metadata as the provided {@code other} schema.
     * </p>
     *
     * @param other The {@code StructSchema} to copy.
     * @throws IllegalArgumentException if {@code other} is null.
     */
    public StructSchema(StructSchema other) {
        this(other.namespace(), other.name(), other.doc(), other.fields);
    }

    /**
     * Constructs a {@code StructSchema} with the specified namespace, name, documentation, and fields.
     *
     * @param namespace The namespace of the schema. May be {@code null}.
     * @param name      The name of the schema. Must not be {@code null} or empty.
     * @param doc       Optional documentation or description of the schema.
     * @param fields    The list of fields that make up the schema. May be empty but not null.
     * @throws IllegalArgumentException if {@code name} is null or empty.
     */
    @Builder(builderMethodName = "builder")
    public StructSchema(String namespace, String name, String doc, @Singular List<DataField> fields) {
        super(DataSchemaConstants.STRUCT_TYPE, namespace, name, doc);
        if (fields != null) {
            this.fields.addAll(fields);
            for (var field : fields) {
                fieldsByName.put(field.name(), field);
            }
        }
    }

    /**
     * Retrieves a field by its index in the schema.
     *
     * @param index The index of the field to retrieve.
     * @return The {@link DataField} at the specified index.
     * @throws IndexOutOfBoundsException if the index is out of range.
     */
    public DataField field(int index) {
        return fields.get(index);
    }

    /**
     * Retrieves a field by its name.
     * <p>
     * This method provides a quick lookup for fields using their names.
     * If no field is found with the requested name, it returns {@code null}.
     * </p>
     *
     * @param name The name of the field to retrieve.
     * @return The {@link DataField} with the specified name, or {@code null} if it doesn't exist.
     */
    public DataField field(String name) {
        return fieldsByName.get(name);
    }

    /**
     * Retrieves the list of fields in the schema.
     * <p>
     * The order of fields in the list is the same as the order specified during schema creation.
     * </p>
     *
     * @return A {@link List} of {@link DataField} objects representing the schema's fields.
     */
    public List<DataField> fields() {
        return Lists.newCopyOnWriteArrayList(fields);
    }

    /**
     * Determines if this schema can be assigned from another schema.
     * <p>
     * This method checks the compatibility of another schema with this structured schema.
     * Compatibility requires the {@code otherSchema} to also be a {@code StructSchema} and
     * its fields must be compatible with the fields in this schema.
     * </p>
     *
     * @param otherSchema The other {@link DataSchema} to check for compatibility.
     * @return {@code true} if the other schema is assignable to this schema, {@code false} otherwise.
     */
    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof StructSchema otherStructSchema)) return false;
        // Ensure the other schema has the same fields with compatible types
        for (var field : fields) {
            // Get the field with the same name from the other schema
            final var otherField = otherStructSchema.field(field.name());
            // If the field exists in the other schema, then validate its compatibility
            if (otherField != null && !field.isAssignableFrom(otherField)) return false;
            // If this field has no default value, then the field should exist in the other schema
            if (field.defaultValue() == null && otherField == null) return false;
        }
        return true;
    }

    /**
     * Compares this object with the specified object for equality.
     * <p>
     * This method checks whether the provided object is of the same type
     * and whether all significant fields of this object are equal to the respective
     * fields of the provided object. It performs a deep comparison of fields to
     * determine structural equivalence.
     * </p>
     *
     * @param other The object to compare with this instance.
     * @return {@code true} if the specified object is equal to this one; {@code false} otherwise.
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (!super.equals(other)) return false;
        StructSchema that = (StructSchema) other;
        if (!this.isAssignableFrom(that)) return false;
        return that.isAssignableFrom(this);
    }

    /**
     * Computes a hash code for this object based on its significant fields.
     * <p>
     * The hash code is computed in such a way that it is consistent with the
     * {@link #equals(Object)} method. That is, if two objects are equal according
     * to the {@code equals} method, they must have the same hash code.
     * </p>
     *
     * @return The hash code value for this object.
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields.hashCode());
    }
}
