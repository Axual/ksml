package io.axual.ksml.notation.string;

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

import java.util.List;
import java.util.Map;

import io.axual.ksml.execution.FatalError;

public class CustomStringMapper implements StringMapper<Object> {
    private static final TypeReference<List<Object>> listReference = new TypeReference<>() {
    };
    private static final TypeReference<Map<String, Object>> mapReference = new TypeReference<>() {
    };
    private final ObjectMapper mapper;

    public CustomStringMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Object fromString(String value) {
        try {
            // Try to parse as an object and return as Map<String,Object>
            return mapper.readValue(value, mapReference);
        } catch (Exception mapException) {
            try {
                // Try to parse as an array and return as List<String>
                return mapper.readValue(value, listReference);
            } catch (Exception listException) {
                throw FatalError.dataError("Could not parse JSON string: " + value);
            }
        }
    }

    @Override
    public String toString(Object value) {
        try {
            return mapper.writer().withRootName("object").writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw FatalError.dataError("Can not convert object to JSON string", e);
        }
    }
}
