package io.axual.ksml.data.object;

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

import io.axual.ksml.data.type.EnumType;
import io.axual.ksml.exception.KSMLExecutionException;

public class DataEnum extends DataPrimitive<String> {
    public DataEnum(String value, EnumType type) {
        super(type, value);
        if (!validateValue(value)) {
            throw new KSMLExecutionException("Invalid enum value for type " + type.schemaName() + ": " + value);
        }
    }

    private boolean validateValue(String value) {
        for (String symbol : ((EnumType) type()).symbols()) {
            if (symbol.equals(value)) return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return type().toString() + ": " + super.toString();
    }
}
