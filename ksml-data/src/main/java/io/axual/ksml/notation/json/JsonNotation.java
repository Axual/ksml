package io.axual.ksml.notation.json;

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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.notation.string.StringMapper;
import io.axual.ksml.notation.string.StringNotation;

public class JsonNotation extends StringNotation {
    public static final String NOTATION_NAME = "JSON";
    private static final JsonDataObjectMapper MAPPER = new JsonDataObjectMapper();

    public JsonNotation() {
        super(new StringMapper<>() {
            @Override
            public DataObject fromString(String value) {
                return MAPPER.toDataObject(value);
            }

            @Override
            public String toString(DataObject value) {
                return MAPPER.fromDataObject(value);
            }
        });
    }

    @Override
    public String name() {
        return NOTATION_NAME;
    }
}
