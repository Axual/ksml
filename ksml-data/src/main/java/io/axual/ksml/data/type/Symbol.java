package io.axual.ksml.data.type;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import static io.axual.ksml.data.schema.DataField.NO_TAG;

public record Symbol(String name, String doc, int tag) {
    public Symbol(String name) {
        this(name, null, NO_TAG);
    }

    public boolean hasDoc() {
        return doc != null && !doc.isEmpty();
    }

    public boolean isAssignableFrom(Symbol other) {
        if (!name.equals(other.name)) return false;
        if (tag == NO_TAG || other.tag == NO_TAG) return true;
        return tag == other.tag;
    }
}
