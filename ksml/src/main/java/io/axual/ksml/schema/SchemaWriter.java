package io.axual.ksml.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SchemaWriter {
    private static final ObjectMapper mapper = new ObjectMapper();

    private SchemaWriter() {
    }

    public static String writeSchema(RecordSchema schema) {
        try {
            return mapper.writeValueAsString(schema);
        } catch (JsonProcessingException e) {
            // Return error as String
            return "SCHEMA_ERROR";
        }
    }

    public static DataSchema readSchema(String schema) {
        try {
            return mapper.readValue(schema, new TypeReference<DataSchema>() {
            });
        } catch (JsonProcessingException e) {
            // Indicate deserialization error by returning null
            return null;
        }
    }
}
