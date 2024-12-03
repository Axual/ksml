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
import io.axual.ksml.data.util.MapUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Getter
@EqualsAndHashCode
public class EnumSchema extends NamedSchema {
    public static final int NO_INDEX = EnumType.NO_INDEX;
    private final Map<String, Integer> symbols;
    private final Map<Integer, String> reverseSymbols;
    private final String defaultValue;

    public EnumSchema(String namespace, String name, String doc, List<String> symbols) {
        this(namespace, name, doc, symbols, null);
    }

    public EnumSchema(String namespace, String name, String doc, Map<String, Integer> symbols) {
        this(namespace, name, doc, symbols, null);
    }

    public EnumSchema(String namespace, String name, String doc, List<String> symbols, String defaultValue) {
        this(namespace, name, doc, MapUtil.listToMap(symbols, NO_INDEX), defaultValue);
    }

    public EnumSchema(String namespace, String name, String doc, Map<String, Integer> symbols, String defaultValue) {
        super(Type.ENUM, namespace, name, doc);
        this.symbols = Collections.unmodifiableMap(symbols);
        final var reverseSymbols = MapUtil.invertMap(symbols);
        reverseSymbols.remove(NO_INDEX);
        this.reverseSymbols = Collections.unmodifiableMap(reverseSymbols);
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
