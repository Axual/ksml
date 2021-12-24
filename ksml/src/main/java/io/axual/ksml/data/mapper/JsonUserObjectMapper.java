package io.axual.ksml.data.mapper;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import io.axual.ksml.data.object.user.UserObject;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.notation.JsonNotation;

public class JsonUserObjectMapper implements UserObjectMapper<String> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final NativeUserObjectMapper nativeMapper = new NativeUserObjectMapper();
    private final TypeReference<List<Object>> listReference = new TypeReference<>() {
    };
    private final TypeReference<Map<String, Object>> mapReference = new TypeReference<>() {
    };

    @Override
    public UserObject toUserObject(UserType expected, String value) {
        try {
            Map<String, Object> map = mapper.readValue(value, mapReference);
            return nativeMapper.toUserObject(JsonNotation.NOTATION_NAME, map);
        } catch (Exception mapException) {
            try {
                List<Object> list = mapper.readValue(value, listReference);
                return nativeMapper.toUserObject(JsonNotation.NOTATION_NAME, list);
            } catch (Exception listException) {
                throw new KSMLParseException("Could not parse JSON string: " + value);
            }
        }
    }

    @Override
    public String fromUserObject(UserObject object) {
        try {
            return mapper.writeValueAsString(nativeMapper.fromUserObject(object));
        } catch (JsonProcessingException e) {
            throw new KSMLExecutionException("Can not convert object to JSON string", e);
        }
    }
}
