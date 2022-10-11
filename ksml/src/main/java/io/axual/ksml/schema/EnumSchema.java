package io.axual.ksml.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import java.util.List;
import java.util.Objects;

public class EnumSchema extends NamedSchema {
    private final List<String> possibleValues;
    private final String defaultValue;

    public EnumSchema(String namespace, String name, String doc, List<String> possibleValues) {
        this(namespace, name, doc, possibleValues, null);
    }

    public EnumSchema(String namespace, String name, String doc, List<String> possibleValues, String defaultValue) {
        super(Type.ENUM, namespace, name, doc);
        this.possibleValues = possibleValues;
        this.defaultValue = defaultValue;
    }

    public List<String> possibleValues() {
        return possibleValues;
    }

    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        if (!super.equals(other)) return false;

        // Compare all schema relevant fields
        if (!Objects.equals(possibleValues, ((EnumSchema) other).possibleValues)) return false;
        return Objects.equals(defaultValue, ((EnumSchema) other).defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), possibleValues, defaultValue);
    }
}
