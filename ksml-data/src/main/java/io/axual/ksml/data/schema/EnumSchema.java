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

import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.data.type.Symbols;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
@EqualsAndHashCode
public class EnumSchema extends NamedSchema {
    private final Symbols symbols;
    private final String defaultValue;

    public EnumSchema(String namespace, String name, String doc, List<String> symbols) {
        this(namespace, name, doc, symbols, null);
    }

    public EnumSchema(String namespace, String name, String doc, Symbols symbols) {
        this(namespace, name, doc, symbols, null);
    }

    public EnumSchema(String namespace, String name, String doc, List<String> symbols, String defaultValue) {
        this(namespace, name, doc, Symbols.from(symbols), defaultValue);
    }

    public EnumSchema(String namespace, String name, String doc, Symbols symbols, String defaultValue) {
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
        for (final var otherSymbol : otherEnum.symbols.entrySet()) {
            if (!symbols.containsKey(otherSymbol.getKey())) return false;
            final var index = symbols.get(otherSymbol.getKey());
            if (!Objects.equals(otherSymbol.getValue(), index)) return false;
        }
        return true;
    }
}
