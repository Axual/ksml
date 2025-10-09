package io.axual.ksml.data.value;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Simple immutable tuple holding an ordered list of elements.
 *
 * <p>Used as the native representation for tuple-like values which can be mapped to
 * {@code DataTuple} and {@code TupleType} in the KSML data model.</p>
 */
@EqualsAndHashCode
@Getter
public class Tuple<T> {
    private final List<T> elements;

    /**
     * Create a tuple from the provided elements. The internal list is unmodifiable.
     *
     * @param elements ordered elements of the tuple
     */
    @SafeVarargs
    public Tuple(T... elements) {
        this.elements = Collections.unmodifiableList(Arrays.asList(elements));
    }

    /**
     * Render as (e0, e1, ...).
     */
    @Override
    public String toString() {
        var sb = new StringBuilder("(");
        for (int index = 0; index < elements.size(); index++) {
            if (index > 0) sb.append(", ");
            sb.append(elements.get(index).toString());
        }
        sb.append(")");
        return sb.toString();
    }
}
