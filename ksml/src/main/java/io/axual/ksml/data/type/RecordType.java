package io.axual.ksml.data.type;

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


import io.axual.ksml.data.object.DataString;
import io.axual.ksml.schema.DataSchema;

public class RecordType extends MapType {
    private final DataSchema schema;

    public RecordType(DataSchema schema) {
        super(DataString.TYPE, DataType.UNKNOWN);
        this.schema = schema;
    }

    public DataSchema schema() {
        return schema;
    }

    @Override
    public String toString() {
        return "Record" + (schema != null ? "<" + schema.name() + ">" : "");
    }

    @Override
    public String schemaName() {
        return "Record" + (schema != null ? "Of" + schema.name() : "");
    }
}
