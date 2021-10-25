package io.axual.ksml.data.type.user;

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


import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.schema.DataSchema;

public class UserRecordType extends UserMapType {
    private final DataSchema schema;

    public UserRecordType(String notation) {
        this(notation, null);
    }

    public UserRecordType(DataSchema schema) {
        this(schema.notation(), schema);
    }

    private UserRecordType(String notation, DataSchema schema) {
        super(notation, new StaticUserType(UserString.TYPE, notation), new StaticUserType(DataType.UNKNOWN, notation),schema);
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
