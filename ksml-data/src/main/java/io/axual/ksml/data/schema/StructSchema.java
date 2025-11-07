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
import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.DataEquals;
import io.axual.ksml.data.compare.Equality;
import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.util.EqualUtil;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_NAMED_SCHEMA_DOC;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_CONSTANT;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_DEFAULT_VALUE;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_DOC;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_NAME;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_ORDER;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_REQUIRED;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_SCHEMA;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_TAG;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_ALLOWED;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_SCHEMA;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_SCHEMA_DOC;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_SCHEMA_FIELDS;
import static io.axual.ksml.data.util.AssignableUtil.fieldNotAssignable;
import static io.axual.ksml.data.util.AssignableUtil.schemaMismatch;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/**
 * Represents a structured schema with named fields in the KSML framework.
 * <p>
 * The {@code StructSchema} class extends {@link NamedSchema} and is used to define
 * schemas for structured data. A structured schema is composed of a set of named fields,
 * where each field has a name, a type (defined by {@link DataSchema}), and optionally additional metadata.
 * </p>
 */
@EqualsAndHashCode
public class StructSchema extends NamedSchema {
    /**
     * Represents a field in a data schema, containing metadata about the field such as its name,
     * schema definition, documentation, and constraints. This class provides functionality
     * to define and validate fields within a schema.
     */
    public record Field(String name, DataSchema schema, String doc, int tag, boolean required, boolean constant,
                        DataObject defaultValue, Order order) implements DataEquals {
        /**
         * Enum representing the sorting order of the field.
         * <ul>
         *     <li>ASCENDING: Field is sorted in ascending order.</li>
         *     <li>DESCENDING: Field is sorted in descending order.</li>
         *     <li>IGNORE: Sorting is ignored.</li>
         * </ul>
         */
        public enum Order {
            ASCENDING, DESCENDING, IGNORE
        }

        /**
         * Constructs a new StructField with specified properties.
         *
         * @param name         The name of the field. Can be null for an anonymous field.
         * @param schema       The schema of the field. Cannot be null.
         * @param doc          The documentation string for the field. Can be null.
         * @param tag          The tag of the field in the schema.
         * @param required     Whether the field is required (mandatory).
         * @param constant     Whether the field is constant and unmodifiable.
         * @param defaultValue The default value of the field. Can be null.
         * @param order        The sorting order of the field (ascending, descending, or ignored).
         */
        public Field(String name, DataSchema schema, String doc, int tag, boolean required, boolean constant, DataObject defaultValue, Order order) {
            Objects.requireNonNull(schema);
            this.name = name;
            this.schema = schema;
            this.doc = doc;
            // Tags are always set on members of unions, not on the unions themselves
            this.tag = schema instanceof UnionSchema ? NO_TAG : tag;
            this.required = required;
            this.constant = constant;
            this.defaultValue = required || defaultValue != null ? defaultValue : DataNull.INSTANCE;
            this.order = order;
        }

        /**
         * Constructs a new anonymous required StructField with no default value.
         *
         * @param schema The schema of the field. Cannot be null.
         */
        public Field(DataSchema schema) {
            this(null, schema);
        }

        /**
         * Constructs a new optional StructField with the specified name and schema.
         *
         * @param name   The name of the field.
         * @param schema The schema of the field. Cannot be null.
         */
        public Field(String name, DataSchema schema) {
            this(name, schema, null);
        }

        /**
         * Constructs a new optional StructField with the specified name, schema, and documentation.
         *
         * @param name   The name of the field.
         * @param schema The schema of the field. Cannot be null.
         * @param doc    The documentation string for the field. Can be null.
         */
        public Field(String name, DataSchema schema, String doc) {
            this(name, schema, doc, NO_TAG);
        }

        /**
         * Constructs a new optional StructField with the specified name, schema, documentation, and tag.
         *
         * @param name   The name of the field.
         * @param schema The schema of the field. Cannot be null.
         * @param doc    The documentation string for the field. Can be null.
         * @param tag    The tag of the field in the schema.
         */
        public Field(String name, DataSchema schema, String doc, int tag) {
            this(name, schema, doc, tag, true, false, null);
        }

        /**
         * Constructs a new StructField with the specified name, schema, documentation, tag, and required status.
         *
         * @param name     The name of the field.
         * @param schema   The schema of the field. Cannot be null.
         * @param doc      The documentation string for the field. Can be null.
         * @param tag      The tag of the field.
         * @param required Whether the field is required.
         */
        public Field(String name, DataSchema schema, String doc, int tag, boolean required) {
            this(name, schema, doc, tag, required, false);
        }

        /**
         * Constructs a new StructField with the specified properties and assigns a default ascending order.
         *
         * @param name     The name of the field.
         * @param schema   The schema of the field. Cannot be null.
         * @param doc      The documentation string for the field. Can be null.
         * @param tag      The tag of the field.
         * @param required Whether the field is required.
         * @param constant Whether the field is constant and unmodifiable.
         */
        public Field(String name, DataSchema schema, String doc, int tag, boolean required, boolean constant) {
            this(name, schema, doc, tag, required, constant, null);
        }

        /**
         * Constructs a new StructField with the specified properties and assigns a default ascending order.
         *
         * @param name         The name of the field.
         * @param schema       The schema of the field. Cannot be null.
         * @param doc          The documentation string for the field. Can be null.
         * @param tag          The tag of the field.
         * @param required     Whether the field is required.
         * @param constant     Whether the field is constant and unmodifiable.
         * @param defaultValue The default value of the field. Can be null.
         */
        public Field(String name, DataSchema schema, String doc, int tag, boolean required, boolean constant, DataObject defaultValue) {
            this(name, schema, doc, tag, required, constant, defaultValue, Order.ASCENDING);
        }

        /**
         * Checks if the field has documentation defined.
         *
         * @return true if documentation is defined and not empty, false otherwise.
         */
        public boolean hasDoc() {
            return doc != null && !doc.isEmpty();
        }

        /**
         * Checks whether the schema of this field is assignable from another field's schema.
         *
         * @param otherField The other field to compare against.
         */
        public Assignable isAssignableFrom(Field otherField) {
            if (otherField == null) {
                return Assignable.notAssignable("Field \"" + name + "\" not found");
            } else {
                return schema.isAssignableFrom(otherField.schema);
            }
        }

        /**
         * Returns a string representation of the StructField, including its name, schema, tag, and required status.
         *
         * @return A string summarizing the field's details.
         */
        @Override
        public String toString() {
            return (name != null ? name : "<anonymous>") + ": " + schema + " (" + tag + (required ? "" : ", optional") + ")";
        }

        /**
         * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
         *
         * @param obj   The other schema to compare.
         * @param flags The flags that indicate what to compare.
         */
        @Override
        public Equality equals(Object obj, EqualityFlags flags) {
            if (this == obj) return Equality.equal();
            if (obj == null) return otherIsNull(this);
            if (!getClass().equals(obj.getClass())) return EqualUtil.containerClassNotEqual(getClass(), obj.getClass());

            final var that = (Field) obj;

            // Compare name
            if (!flags.isSet(IGNORE_STRUCT_FIELD_NAME) && !Objects.equals(name, that.name))
                return fieldNotEqual("name", this, name, that, that.name);

            // Compare schema
            if (!flags.isSet(IGNORE_STRUCT_FIELD_SCHEMA)) {
                final var schemaEqual = schema.equals(that.schema, flags);
                if (schemaEqual.isNotEqual())
                    return fieldNotEqual("schema", this, schema, that, that.schema, schemaEqual);
            }

            // Compare doc
            if (!flags.isSet(IGNORE_STRUCT_FIELD_DOC) && !Objects.equals(doc, that.doc))
                return fieldNotEqual("doc", this, doc, that, that.doc);

            // Compare required
            if (!flags.isSet(IGNORE_STRUCT_FIELD_REQUIRED) && !Objects.equals(required, that.required))
                return fieldNotEqual("required", this, required, that, that.required);

            // Compare constant
            if (!flags.isSet(IGNORE_STRUCT_FIELD_CONSTANT) && !Objects.equals(constant, that.constant))
                return fieldNotEqual("constant", this, constant, that, that.constant);

            // Compare tag
            if (!flags.isSet(IGNORE_STRUCT_FIELD_TAG) && !Objects.equals(tag, that.tag))
                return fieldNotEqual("tag", this, tag, that, that.tag);

            // Compare defaultValue
            if (!flags.isSet(IGNORE_STRUCT_FIELD_DEFAULT_VALUE) && !Objects.equals(defaultValue, that.defaultValue))
                return fieldNotEqual("defaultValue", this, defaultValue, that, that.defaultValue);

            // Compare order
            if (!flags.isSet(IGNORE_STRUCT_FIELD_ORDER) && !Objects.equals(order, that.order))
                return fieldNotEqual("order", this, order, that, that.order);

            return Equality.equal();
        }
    }

    /**
     * This instance exists for compatibility reasons: if we define eg. JSON Objects, or schema-less Structs, then we
     * need to somehow capture this in a schema with the proper type. The StructSchema is the proper type, so we let
     * the absence of a schema be reflected through null fields. Only 1 instance without a name is allowed, so code
     * that checks if the StructSchema represents "schemaless" can simply perform an equality ('==') check.
     */
    public static final StructSchema SCHEMALESS = new StructSchema(null, null, null, null, null, null);

    /**
     * A list of fields that make up the structured schema.
     * <p>
     * Each field is defined by a {@link Field} object, which includes its name and schema definition.
     * The order of fields in this list is preserved.
     * </p>
     */
    private final List<Field> fields = new ArrayList<>();
    /**
     * A map of field names to their corresponding {@link Field} objects.
     * <p>
     * This map provides efficient access to fields by their names.
     * </p>
     */
    private final Map<String, Field> fieldsByName = new HashMap<>();

    /**
     * Indicates if the data structure is allowed to have additional fields.
     * <p>
     * If the true then the {@link #additionalFieldsSchema} can be used to determine the type of those fields
     * </p>
     */
    @Getter
    private final boolean additionalFieldsAllowed;

    /**
     * The field that sets which type of value additional fields can have. Only the Schema part will be used.
     * <p>
     * If {@link #additionalFieldsAllowed} is true this type will be used. If the value is {@link DataSchema#ANY_SCHEMA} any type of value is allowed.
     * </p>
     */
    @Getter
    private final DataSchema additionalFieldsSchema;

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
        this(other.namespace(), other.name(), other.doc(), other.fields(), other.additionalFieldsAllowed(), other.additionalFieldsSchema());
    }

    /**
     * Constructs a {@code StructSchema} with the specified namespace, name, documentation, and fields.
     *
     * @param fields The list of fields that make up the schema. May be empty but not null.
     * @throws IllegalArgumentException if {@code name} is null or empty.
     */
    public StructSchema(@Singular List<Field> fields) {
        this(null, null, null, fields, true);
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
    public StructSchema(String namespace, String name, String doc, @Singular List<Field> fields) {
        this(namespace, name, doc, fields, true);
    }

    /**
     * Constructs a {@code StructSchema} with the specified namespace, name, documentation, and fields.
     *
     * @param namespace               The namespace of the schema. May be {@code null}.
     * @param name                    The name of the schema. Must not be {@code null} or empty.
     * @param doc                     Optional documentation or description of the schema.
     * @param fields                  The list of fields that make up the schema. May be empty but not null.
     * @param additionalFieldsAllowed set to true or null to allow additional fields to be used in this struct
     * @throws IllegalArgumentException if {@code name} is null or empty.
     */
    public StructSchema(String namespace, String name, String doc, @Singular List<Field> fields, boolean additionalFieldsAllowed) {
        this(namespace, name, doc, fields, additionalFieldsAllowed, null);
    }

    /**
     * Constructs a {@code StructSchema} with the specified namespace, name, documentation, and fields.
     *
     * @param namespace               The namespace of the schema. May be {@code null}.
     * @param name                    The name of the schema. Must not be {@code null} or empty.
     * @param doc                     Optional documentation or description of the schema.
     * @param fields                  The list of fields that make up the schema. Maybe empty but not null.
     * @param additionalFieldsAllowed set to true or null to allow additional fields in this struct
     * @param additionalFieldsSchema  Use a {@link DataSchema} to limit any additional fields to a specific schema
     * @throws IllegalArgumentException if {@code name} is null or empty.
     */
    @Builder(builderMethodName = "builder")
    public StructSchema(String namespace, String name, String doc, @Singular List<Field> fields, Boolean additionalFieldsAllowed, DataSchema additionalFieldsSchema) {
        super(DataSchemaConstants.STRUCT_TYPE, namespace, name, doc);
        if (fields != null) {
            this.fields.addAll(fields);
            for (var field : fields) {
                fieldsByName.put(field.name(), field);
            }
        }
        this.additionalFieldsAllowed = additionalFieldsAllowed == null || additionalFieldsAllowed;
        if (this.additionalFieldsAllowed) {
            this.additionalFieldsSchema = additionalFieldsSchema != null ? additionalFieldsSchema : ANY_SCHEMA;
        } else {
            this.additionalFieldsSchema = null;
        }
    }

    /**
     * Retrieves a field by its index in the schema.
     *
     * @param index The index of the field to retrieve.
     * @return The {@link Field} at the specified index.
     * @throws IndexOutOfBoundsException if the index is out of range.
     */
    public Field field(int index) {
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
     * @return The {@link Field} with the specified name, or {@code null} if it doesn't exist.
     */
    public Field field(String name) {
        return fieldsByName.get(name);
    }

    /**
     * Retrieves the list of fields in the schema.
     * <p>
     * The order of fields in the list is the same as the order specified during schema creation.
     * </p>
     *
     * @return A {@link List} of {@link Field} objects representing the schema's fields.
     */
    public List<Field> fields() {
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
     */
    @Override
    public Assignable isAssignableFrom(DataSchema otherSchema) {
        final var superAssignable = super.isAssignableFrom(otherSchema);
        if (superAssignable.isNotAssignable()) return superAssignable;
        if (!(otherSchema instanceof StructSchema that))
            return schemaMismatch(this, otherSchema);
        // Ensure the other schema has the same fields with compatible types
        for (final var field : fields) {
            // Get the field with the same name from the other schema
            final var thatField = that.field(field.name());
            // If the field exists in the other schema, then validate its compatibility
            if (thatField != null) {
                final var fieldAssignable = field.isAssignableFrom(thatField);
                if (fieldAssignable.isNotAssignable())
                    return fieldNotAssignable(field.name(), this, field, that, thatField, fieldAssignable);
            }
            // If this field has no default value, then the field should exist in the other schema
            if (field.defaultValue() == null && thatField == null) {
                return Assignable.notAssignable("Other schema does not contain required field \"" + field.name() + "\"");
            }
        }
        // All fields are assignable, so return no error
        return Assignable.assignable();
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param obj   The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Equality equals(Object obj, EqualityFlags flags) {
        // If a struct-specific flag is set to ignore doc, set the general flag for the superclass before calling it
        final var superEqual = super.equals(obj, flags.ifSetThenAdd(IGNORE_STRUCT_SCHEMA_DOC, IGNORE_NAMED_SCHEMA_DOC));
        if (superEqual.isNotEqual()) return superEqual;

        final var that = (StructSchema) obj;

        // Compare additionalFieldsAllowed
        if (!flags.isSet(IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_ALLOWED) && !Objects.equals(additionalFieldsAllowed, that.additionalFieldsAllowed))
            return fieldNotEqual("additionalFieldsAllowed", this, additionalFieldsAllowed, that, that.additionalFieldsAllowed);

        // Compare additionalFieldsSchema
        if (!flags.isSet(IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_SCHEMA) && (additionalFieldsSchema != null || that.additionalFieldsSchema != null)) {
            if (additionalFieldsSchema == null || that.additionalFieldsSchema == null)
                return fieldNotEqual("additionalFieldsSchema", this, additionalFieldsSchema, that, that.additionalFieldsSchema);
            final var additionalFieldsSchemaEqual = additionalFieldsSchema.equals(that.additionalFieldsSchema, flags);
            if (additionalFieldsSchemaEqual.isNotEqual())
                return fieldNotEqual("additionalFieldsSchema", this, additionalFieldsSchema, that, that.additionalFieldsSchema, additionalFieldsSchemaEqual);
        }

        // Compare fields
        if (!flags.isSet(IGNORE_STRUCT_SCHEMA_FIELDS)) {
            if (fields.size() != that.fields.size())
                return fieldNotEqual("fieldCount", this, fields.size(), that, that.fields.size());

            for (int i = 0; i < fields.size(); i++) {
                final var fieldEqual = fields.get(i).equals(that.fields.get(i), flags);
                if (fieldEqual.isNotEqual())
                    return fieldNotEqual("field[" + i + "]", this, fields.get(i), that, that.fields.get(i), fieldEqual);
            }
        }

        return Equality.equal();
    }
}
