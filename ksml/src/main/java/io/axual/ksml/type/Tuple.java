package io.axual.ksml.type;

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



// This class implements a Tuple with any number of elements
public class Tuple {
    private final Object[] elements;

    public Tuple(Object... elements) {
        this.elements = new Object[elements.length];
        System.arraycopy(elements, 0, this.elements, 0, elements.length);
    }

    public Object get(int index) {
        return elements[index];
    }

    public int size() {
        return elements.length;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        Tuple o = (Tuple) other;
        if (elements.length != o.elements.length) return false;
        for (int index = 0; index < elements.length; index++) {
            if (elements[index] == null && o.elements[index] == null)
                continue;
            if (elements[index] == null || o.elements[index] == null) return false;
            if (!elements[index].equals(o.elements[index])) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = result * 31 + elements.length;
        for (Object field : elements) {
            result = result * 31 + (field != null ? field.hashCode() : 1234);
        }
        return result;
    }
}
