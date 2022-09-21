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

import io.axual.ksml.data.type.RecordType;
import io.axual.ksml.schema.RecordSchema;

public class DataRecord extends HashMap<String, DataObject> implements DataObject {
    private final transient RecordType type;

    public DataRecord(RecordSchema schema) {
        type = new RecordType(schema);
    }

    @Override
    public RecordType type() {
        return type;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (!(other instanceof DataRecord)) return false;
        return type.equals(((DataRecord) other).type);
    }


    @Override
    public int hashCode() {
        return type.hashCode() + super.hashCode() * 31;
    }
}
