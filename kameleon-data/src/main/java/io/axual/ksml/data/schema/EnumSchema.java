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

import java.util.Collections;
import java.util.List;

@Getter
@EqualsAndHashCode
public class EnumSchema extends NamedSchema {
    private final List<String> symbols;
    private final String defaultValue;

    public EnumSchema(String namespace, String name, String doc, List<String> possibleValues) {
        this(namespace, name, doc, possibleValues, null);
    }

    public EnumSchema(String namespace, String name, String doc, List<String> symbols, String defaultValue) {
        super(Type.ENUM, namespace, name, doc);
        this.symbols = Collections.unmodifiableList(symbols);
        this.defaultValue = defaultValue;
    }

    @Override
    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (!super.isAssignableFrom(otherSchema)) return false;
        if (!(otherSchema instanceof EnumSchema enumSchema)) return false;
        // This schema is assignable from the other schema when the list of symbols is a superset
        // of the otherSchema's set of symbols.
        for (String symbol : enumSchema.symbols) {
            if (!symbols.contains(symbol)) return false;
        }
        return true;
    }
}
