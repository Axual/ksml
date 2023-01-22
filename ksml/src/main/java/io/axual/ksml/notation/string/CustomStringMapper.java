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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axual.ksml.exception.KSMLDataException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.binary.NativeJsonNodeMapper;

import java.io.IOException;
import java.io.StringWriter;

public class CustomStringMapper implements StringMapper<Object> {
    protected final ObjectMapper mapper;
    private static final NativeJsonNodeMapper NATIVE_MAPPER = new NativeJsonNodeMapper();

    public CustomStringMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Object fromString(String value) {
        try {
            var tree = mapper.readTree(value);
            return NATIVE_MAPPER.fromJsonNode(tree);
        } catch (Exception mapException) {
            throw new KSMLDataException("Could not parse string to object: " + (value != null ? value : "null"));
        }
    }

    @Override
    public String toString(Object value) {
        try {
            final var writer = new StringWriter();
            mapper.writeTree(mapper.createGenerator(writer), NATIVE_MAPPER.toJsonNode(value));
            return writer.toString();
        } catch (IOException e) {
            throw FatalError.dataError("Can not convert object to JSON string: " + (value != null ? value.toString() : "null"), e);
        }
    }
}
