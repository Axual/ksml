package io.axual.ksml.data.type;

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

import io.axual.ksml.data.compare.Compared;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.util.ListUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A {@link SimpleType} representing an enumeration of allowed string symbols.
 * <p>
 * Values are considered assignable only when they are strings that match one of the configured
 * {@link EnumSymbol} entries.
 */
@Getter
@EqualsAndHashCode
public class EnumType extends SimpleType {
    private final EnumSchema schema;

    public EnumType(EnumSchema schema) {
        super(String.class, schema.name(), DataSchemaConstants.ENUM_TYPE);
        this.schema = schema;
    }

    @Override
    public Compared checkAssignableFrom(DataObject value) {
        final var superVerified = super.checkAssignableFrom(value);
        if (superVerified.isError()) return superVerified;
        final var valueStr = value.toString();
        if (ListUtil.find(schema.symbols(), s -> s.name().equals(valueStr)) != null) return Compared.ok();
        final var symbolsStr = schema.symbols().stream().map(s -> "\"" + s.name() + "\"").toList();
        return Compared.error("Symbol \"" + valueStr + "\" not found in enumeration with symbols " + String.join(", ", symbolsStr));
    }
}
