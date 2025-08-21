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
     * @return {@code true} if the given schema is compatible; otherwise, {@code false}.
     */
    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        // Always allow assigning from a string value (assuming a valid symbol)
        if (otherSchema == DataSchema.STRING_SCHEMA) return true; // Strings are convertable to ENUM

        // Check super's compatibility
        if (!super.isAssignableFrom(otherSchema)) return false;

        // If okay then check class compatibility
        if (!(otherSchema instanceof EnumSchema otherEnum)) return false;

        // This schema is assignable from the other enum when the map of symbols is a superset
        // of the otherEnum's set of symbols.
        for (final var otherSymbol : otherEnum.symbols) {
            // Validate that the other symbol is present and equal in our own symbol list
            if (ListUtil.find(symbols, thisSymbol -> thisSymbol.isAssignableFrom(otherSymbol)) == null)
                return false;
        }

        // When we reach this point, it means that we can be assigned any
        // value from the otherEnum.
        return true;
    }
}
