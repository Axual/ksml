package io.axual.ksml.data.notation;

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

import java.util.HashMap;
import java.util.Map;

public class NotationLibrary {
    private NotationLibrary() {
    }

    private static final Map<String, Notation> notationEntries = new HashMap<>();

    public static void register(String name, Notation notation) {
        notationEntries.put(name, notation);
    }

    public static boolean exists(String notation) {
        return notationEntries.containsKey(notation);
    }

    public static Notation notation(String notation) {
        var result = notation != null ? notationEntries.get(notation) : null;
        if (result != null) return result;
        throw new DataException("Data notation is not registered in the NotationLibrary: " + (notation != null ? notation : "null"));
    }
}
