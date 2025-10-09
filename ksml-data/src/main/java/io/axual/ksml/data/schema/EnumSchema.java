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
import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.compare.DataEquals;
import io.axual.ksml.data.type.Flags;
import io.axual.ksml.data.util.EqualUtil;
import io.axual.ksml.data.util.ListUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;
import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_ENUM_SCHEMA_DEFAULT_VALUE;
import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_ENUM_SCHEMA_SYMBOLS;
import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_ENUM_SCHEMA_SYMBOL_DOC;
import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_ENUM_SCHEMA_SYMBOL_NAME;
import static io.axual.ksml.data.schema.DataSchemaFlags.IGNORE_ENUM_SCHEMA_SYMBOL_TAG;
import static io.axual.ksml.data.util.AssignableUtil.schemaMismatch;
import static io.axual.ksml.data.util.EqualUtil.fieldNotEqual;
import static io.axual.ksml.data.util.EqualUtil.otherIsNull;

/**
 * Represents a named schema for enumerations in the KSML framework.
 * <p>
 * The {@code EnumSchema} class extends {@link NamedSchema} and is specifically designed
 * to model enumerations. It encapsulates a set of symbols (possible values for the enumeration)
 * and optionally includes a default value.
 * </p>
 * <p>
 * This schema is used in cases where a predefined set of acceptable values is required.
 * </p>
 * <p>
 * Values are considered assignable only when they are strings that match one of the configured
 * {@link Symbol} entries.
 * </p>
 */
@Getter
@EqualsAndHashCode
public class EnumSchema extends NamedSchema {
    public record Symbol(String name, String doc, int tag) implements DataEquals {
        public Symbol(String name, String doc, Integer tag) {
            this(name, doc, tag != null ? tag : NO_TAG);
        }

        public Symbol(String name) {
            this(name, null, NO_TAG);
        }

        public boolean hasDoc() {
            return doc != null && !doc.isEmpty();
        }

        public boolean isAssignableFrom(Symbol other) {
            if (!name.equals(other.name)) return false;
            if (tag == NO_TAG || other.tag == NO_TAG) return true;
            return tag == other.tag;
        }

        public static Symbol of(String symbol) {
            return new Symbol(symbol);
        }

        @Override
        public Equal equals(Object other, Flags flags) {
            if (this == other) return Equal.ok();
            if (other == null) return otherIsNull(this);
            if (!getClass().equals(other.getClass()))
                return EqualUtil.containerClassNotEqual(getClass(), other.getClass());

            final var that = (Symbol) other;

            // Compare name
            if (!flags.isSet(IGNORE_ENUM_SCHEMA_SYMBOL_NAME) && !Objects.equals(name, that.name))
                return fieldNotEqual("name", this, name, that, that.name);

            // Compare schema
            if (!flags.isSet(IGNORE_ENUM_SCHEMA_SYMBOL_DOC) && !Objects.equals(doc, that.doc))
                return fieldNotEqual("doc", this, doc, that, that.doc);

            // Compare tag
            if (!flags.isSet(IGNORE_ENUM_SCHEMA_SYMBOL_TAG) && !Objects.equals(tag, that.tag))
                return fieldNotEqual("tag", this, tag, that, that.tag);

            return Equal.ok();
        }
    }

    /**
     * The list of symbols that define the valid values for this enumeration schema.
     */
    private final List<Symbol> symbols;
    /**
     * The optional default value for the enumeration.
     * <p>
     * If no explicit value is provided, this can be {@code null}.
     * </p>
     */
    private final Symbol defaultValue;

    /**
     * Constructs a new {@code EnumSchema} with the given namespace, name, documentation, and symbols.
     * <p>
     * This constructor creates an enumeration schema without a default value.
     * </p>
     *
     * @param symbols The list of symbols (values) allowed in this enumeration schema.
     */
    public EnumSchema(List<Symbol> symbols) {
        this(null, DataSchemaConstants.ENUM_TYPE, null, symbols, null);
    }

    /**
     * Constructs a new {@code EnumSchema} with the given namespace, name, documentation, and symbols.
     * <p>
     * This constructor creates an enumeration schema without a default value.
     * </p>
     *
     * @param namespace The namespace of the schema.
     * @param name      The name of the schema.
     * @param doc       The documentation or description associated with the schema.
     * @param symbols   The list of symbols (values) allowed in this enumeration schema.
     */
    public EnumSchema(String namespace, String name, String doc, List<Symbol> symbols) {
        this(namespace, name, doc, symbols, null);
    }

    /**
     * Constructs a new {@code EnumSchema} with the given namespace, name, documentation, symbols, and default value.
     *
     * @param namespace    The namespace of the schema.
     * @param name         The name of the schema.
     * @param doc          The documentation or description associated with the schema.
     * @param symbols      The list of symbols (values) allowed in this enumeration schema.
     * @param defaultValue The optional default value for this schema.
     */
    public EnumSchema(String namespace, String name, String doc, List<Symbol> symbols, Symbol defaultValue) {
        super(DataSchemaConstants.ENUM_TYPE, namespace, name, doc);
        this.symbols = symbols;
        this.defaultValue = defaultValue;
    }

    /**
     * Checks whether this schema is assignable from the given schema.
     * <p>
     * This method determines if another schema is compatible with this enumeration schema.
     * </p>
     *
     * @param otherSchema The schema to be checked for compatibility.
     */
    @Override
    public Assignable isAssignableFrom(DataSchema otherSchema) {
        // Always allow assigning from a string value (assuming a valid symbol)
        if (otherSchema == DataSchema.STRING_SCHEMA) return Assignable.ok();
        // Check super's compatibility
        final var superAssignable = super.isAssignableFrom(otherSchema);
        if (superAssignable.isError()) return superAssignable;
        // Check class compatibility
        if (!(otherSchema instanceof EnumSchema otherEnum)) return schemaMismatch(this, otherSchema);
        // This schema is assignable from the other enum when the map of symbols is equal or a superset of the
        // otherEnum's set of symbols.
        for (final var otherSymbol : otherEnum.symbols) {
            // Validate that the other symbol is present and equal in our own symbol list
            if (ListUtil.find(symbols, thisSymbol -> thisSymbol.isAssignableFrom(otherSymbol)) == null) {
                return Assignable.error("Symbol \"" + otherSymbol.name() + "\" not found in enumeration");
            }
        }
        // All symbols from the other union are contained within this one, so return no error
        return Assignable.ok();
    }

    /**
     * Checks if this schema type is equal to another schema. Equality checks are parameterized by flags passed in.
     *
     * @param obj   The other schema to compare.
     * @param flags The flags that indicate what to compare.
     */
    @Override
    public Equal equals(Object obj, Flags flags) {
        final var superEqual = super.equals(obj, flags);
        if (superEqual.isError()) return superEqual;

        final var that = (EnumSchema) obj;

        // Compare symbols
        if (!flags.isSet(IGNORE_ENUM_SCHEMA_SYMBOLS)) {
            // Two unions are equal if their members are all equal
            if (symbols.size() != that.symbols.size())
                return fieldNotEqual("symbolCount", this, symbols.size(), that, that.symbols.size());
            for (int index = 0; index < symbols.size(); index++) {
                final var symbolEqual = symbols.get(index).equals(that.symbols.get(index), flags);
                if (symbolEqual.isError())
                    return fieldNotEqual("symbol[" + index + "]", this, symbols.get(index), that, that.symbols.get(index), symbolEqual);
            }
        }

        // Compare defaultValue
        if (!flags.isSet(IGNORE_ENUM_SCHEMA_DEFAULT_VALUE) && !Objects.equals(defaultValue, that.defaultValue))
            return fieldNotEqual("defaultValue", this, defaultValue, that, that.defaultValue);

        return Equal.ok();
    }
}
