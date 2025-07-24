package io.axual.ksml.execution;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.notation.Notation;

import java.util.HashMap;
import java.util.Map;

public class NotationLibrary {
    private final Map<String, Notation> notations = new HashMap<>();

    public void register(String notationName, Notation notation) {
        notations.put(notationName != null ? notationName.toUpperCase() : null, notation);
    }

    public boolean exists(String notationName) {
        return notations.containsKey(notationName != null ? notationName.toUpperCase() : null);
    }

    public Notation get(String notationName) {
        final var result = getIfExists(notationName);
        if (result != null) return result;
        throw new DataException("Unknown data notation: " + (notationName != null ? notationName : "null"));
    }

    public Notation getIfExists(String notationName) {
        return notations.get(notationName != null ? notationName.toUpperCase() : null);
    }
}
