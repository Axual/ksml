package io.axual.ksml.python;

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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.HashMap;
import java.util.Map;

public class AvroObject implements GenericRecord {
    private final Schema schema;
    private final Map<String, Object> data = new HashMap<>();

    public AvroObject(Schema schema, Map<String, Object> data) {
        this.schema = schema;
        schema.getFields().forEach(field -> {
            if (field.schema().getType() == Schema.Type.ENUM) {
                this.data.put(field.name(), new GenericData.EnumSymbol(field.schema(), (String) data.get(field.name())));
            } else {
                this.data.put(field.name(), data.get(field.name()));
            }
        });
    }

    @Override
    public void put(String key, Object value) {
        data.put(key, value);
    }

    @Override
    public Object get(String key) {
        return data.get(key);
    }

    @Override
    public void put(int index, Object value) {
        data.put(schema.getFields().get(index).name(), value);
    }

    @Override
    public Object get(int index) {
        return data.get(schema.getFields().get(index).name());
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
