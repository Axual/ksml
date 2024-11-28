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

import io.axual.ksml.data.parser.schema.DataSchemaDSL;
import io.axual.ksml.data.util.MapUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;

@Getter
@EqualsAndHashCode
public class EnumType extends SimpleType {
    public static final int NO_INDEX = -1;
    private final Map<String, Integer> symbols;

    public EnumType(String... symbols) {
        this(MapUtil.arrayToMap(symbols, NO_INDEX));
    }

    public EnumType(Map<String, Integer> symbols) {
        super(String.class, DataSchemaDSL.ENUM_TYPE);
        this.symbols = symbols;
    }
}
