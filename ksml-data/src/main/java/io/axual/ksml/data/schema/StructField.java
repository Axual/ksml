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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.compare.DataEquals;
import io.axual.ksml.data.compare.Equality;
import io.axual.ksml.data.compare.EqualityFlags;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.util.EqualUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_CONSTANT;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_DEFAULT_VALUE;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_DOC;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_NAME;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_ORDER;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_REQUIRED;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_SCHEMA;
import static io.axual.ksml.data.schema.DataSchemaFlag.IGNORE_STRUCT_FIELD_TAG;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/**
 * Represents a field in a data schema, containing metadata about the field such as its name,
 * schema definition, documentation, and constraints. This class provides functionality
 * to define and validate fields within a schema.
 */
@Getter
@EqualsAndHashCode
public class StructField implements DataEquals {
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
     * The name of the field. Maybe null for anonymous fields.
     */
    private final String name;
    /**
     * The schema describing the type and structure of this field. This is a
     * mandatory attribute and cannot be null.
     */
    private final DataSchema schema;
    /**
     * An optional description or documentation string for the field. This allows
     * users to provide additional context or usage details for the field.
     */
    private final String doc;
    /**
     * A boolean indicating if the field is considered mandatory.
     * If {@code true}, the field must have a value.
     */
    private final boolean required;
    /**
     * A boolean indicating if the field is constant, meaning its value cannot
     * be changed after being initialized.
     */
    private final boolean constant;
    /**
     * The tag of the field in the schema. Defaults to NO_TAG if
     * not specified.
     */
    private final int tag;
    /**
     * The default value assigned to the field, if any. Can be null if no
     * default is defined.
     */
    private final DataObject defaultValue;
    /**
     * The sorting order for the field, which is one of the {@link Order} enum values.
     * This determines how the field should be sorted when ordering is required.
     */
    private final Order order;

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
    public StructField(String name, DataSchema schema, String doc, int tag, boolean required, boolean constant, DataObject defaultValue, Order order) {
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
    public StructField(DataSchema schema) {
        this(null, schema);
    }

    /**
     * Constructs a new optional StructField with the specified name and schema.
     *
     * @param name   The name of the field.
     * @param schema The schema of the field. Cannot be null.
     */
    public StructField(String name, DataSchema schema) {
        this(name, schema, null);
    }

    /**
     * Constructs a new optional StructField with the specified name, schema, and documentation.
     *
     * @param name   The name of the field.
     * @param schema The schema of the field. Cannot be null.
     * @param doc    The documentation string for the field. Can be null.
     */
    public StructField(String name, DataSchema schema, String doc) {
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
    public StructField(String name, DataSchema schema, String doc, int tag) {
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
    public StructField(String name, DataSchema schema, String doc, int tag, boolean required) {
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
    public StructField(String name, DataSchema schema, String doc, int tag, boolean required, boolean constant) {
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
    public StructField(String name, DataSchema schema, String doc, int tag, boolean required, boolean constant, DataObject defaultValue) {
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
    public Assignable isAssignableFrom(StructField otherField) {
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

        final var that = (StructField) obj;

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
