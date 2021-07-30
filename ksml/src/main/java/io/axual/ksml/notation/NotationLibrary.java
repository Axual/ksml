package io.axual.ksml.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import java.util.HashMap;
import java.util.Map;

import io.axual.ksml.exception.KSMLExecutionException;

public class NotationLibrary {
    private final Map<String, Notation> notations = new HashMap<>();

    public NotationLibrary(Map<?, ?> configs) {
        notations.put(AvroNotation.NAME, new AvroNotation((Map<String, Object>) configs));
        notations.put(JsonNotation.NAME, new JsonNotation((Map<String, Object>) configs));
        notations.put(BinaryNotation.NAME, new BinaryNotation((Map<String, Object>) configs, notations.get(JsonNotation.NAME)));
    }

    // Note: this method is public to facilitate testing
    public void put(String notationName, Notation notation) {
        notations.put(notationName, notation);
    }

    public Notation get(String notation) {
        var result = notations.get(notation);
        if (result != null) return result;
        throw new KSMLExecutionException("Data type notation not found: " + notation);
    }
}
