package io.axual.ksml.data.type;

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

import java.util.Arrays;
import java.util.Objects;

public class EnumType extends SimpleType {
    private final String[] symbols;

    public EnumType(String... symbols) {
        super(String.class);
        this.symbols = symbols;
    }

    public String[] symbols() {
        return symbols;
    }

    @Override
    public String schemaName() {
        return "Enum";
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (getClass() != other.getClass()) return false;
        return Arrays.equals(symbols, ((EnumType) other).symbols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(symbols));
    }
}
