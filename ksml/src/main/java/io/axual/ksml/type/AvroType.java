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



import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroType extends SimpleType {
    public final String schemaName;
    public final Schema schema;

    public AvroType(String schemaName, Schema schema) {
        super(GenericRecord.class);
        this.schemaName = schemaName;
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "avro:" + schemaName;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        return schemaName.equals(((AvroType) other).schemaName);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return result * 31 + schemaName.hashCode();
    }
}
