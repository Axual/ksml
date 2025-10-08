package io.axual.ksml.data.type;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import java.util.Objects;

public class AssignableChecker {
    private Compared isAssignableFrom(DataType a, DataType b) {
        Objects.requireNonNull(a, "No data type specified, this is a bug in KSML");
        Objects.requireNonNull(b, "No data type specified, this is a bug in KSML");
        if (a instanceof SimpleType simpleTypeA) {
            if (!(b instanceof SimpleType simpleTypeB)) return Compared.typeMismatch(simpleTypeA, b);
            return isAssignableFrom(simpleTypeA, simpleTypeB);
        }
        if (a instanceof ComplexType complexTypeA) {
            if (!(b instanceof ComplexType complexTypeB)) return Compared.typeMismatch(complexTypeA, b);
            return isAssignableFrom(complexTypeA, complexTypeB);
        }
        return Compared.error("Unknown data type \"" + a + "\"");
    }

    private Compared isAssignableFrom(SimpleType a, SimpleType b) {
        if (!a.containerClass().isAssignableFrom(b.containerClass())) return Compared.typeMismatch(a, b);
        return Compared.ok();
    }

    private Compared isAssignableFrom(ComplexType a, ComplexType b) {
        if (!a.containerClass().isAssignableFrom(b.containerClass())) return Compared.typeMismatch(a, b);
        if (a.subTypeCount() != b.subTypeCount())
            return Compared.error("Type \"" + b + "\" has a different number of sub-types than \"" + a + "\"");

        for (int i = 0; i < a.subTypeCount(); i++) {
            final var subTypeVerified = isAssignableFrom(a.subType(i), b.subType(i));
            if (subTypeVerified.isError()) return subTypeVerified;
        }
        return Compared.ok();
    }
}
