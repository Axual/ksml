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

import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.data.util.ListUtil;
import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

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
 */
@Getter
@EqualsAndHashCode
public class EnumSchema extends NamedSchema {
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
     * @param context     The validation context.
     */
    @Override
    public ValidationResult checkAssignableFrom(DataSchema otherSchema, ValidationContext context) {
        // Always allow assigning from a string value (assuming a valid symbol)
        if (otherSchema == DataSchema.STRING_SCHEMA) return context.ok();
        // Check super's compatibility
        if (!super.checkAssignableFrom(otherSchema, context).isOK()) return context;
        // Check class compatibility
        if (!(otherSchema instanceof EnumSchema otherEnum)) return context.schemaMismatch(this, otherSchema);
        // This schema is assignable from the other enum when the map of symbols is equal or a superset of the
        // otherEnum's set of symbols.
        for (final var otherSymbol : otherEnum.symbols) {
            // Validate that the other symbol is present and equal in our own symbol list
            if (ListUtil.find(symbols, thisSymbol -> thisSymbol.isAssignableFrom(otherSymbol)) == null) {
                return context.addError("Symbol \"" + otherSymbol.name() + "\" not found in enumeration");
            }
        }
        // All symbols from the other union are contained within this one, so return no error
        return context.ok();
    }
}
