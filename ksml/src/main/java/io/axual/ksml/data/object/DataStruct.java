package io.axual.ksml.data.object;

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

import io.axual.ksml.data.type.StructType;

public class DataStruct extends HashMap<String, DataObject> implements DataObject {
    private final transient StructType type;

    public DataStruct(StructType type) {
        this.type = type;
    }

    @Override
    public StructType type() {
        return type;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (!(other instanceof DataStruct)) return false;
        return type.equals(((DataStruct) other).type);
    }

    @Override
    public int hashCode() {
        return type.hashCode() + super.hashCode() * 31;
    }
}
