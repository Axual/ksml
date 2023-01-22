package io.axual.ksml.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import com.networknt.schema.JsonSchema;
import io.axual.ksml.data.mapper.DataSchemaMapper;
import io.axual.ksml.data.schema.DataSchema;

public class JsonSchemaMapper implements DataSchemaMapper<JsonSchema> {
    @Override
    public DataSchema toDataSchema(JsonSchema value) {
        return null;
    }

    @Override
    public Object fromDataSchema(DataSchema value) {
        return null;
    }
}
