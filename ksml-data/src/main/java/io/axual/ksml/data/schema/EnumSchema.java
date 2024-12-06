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

@Getter
@EqualsAndHashCode
public class EnumSchema extends NamedSchema {
    private final List<Symbol> symbols;
    private final String defaultValue;

    public EnumSchema(String namespace, String name, String doc, List<Symbol> symbols) {
        this(namespace, name, doc, symbols, null);
    }

    public EnumSchema(String namespace, String name, String doc, List<Symbol> symbols, String defaultValue) {
        super(Type.ENUM, namespace, name, doc);
        this.symbols = symbols;
        this.defaultValue = defaultValue;
    }

    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof EnumSchema otherEnum)) return false;
        // This schema is assignable from the other enum when the map of symbols is a superset
        // of the otherEnum's set of symbols.
        for (final var otherSymbol : otherEnum.symbols) {
            // Validate that the other symbol is present and equal in our own symbol list
            if (ListUtil.find(symbols, s -> s.equals(otherSymbol)) == null) return false;
        }
        return true;
    }
}
