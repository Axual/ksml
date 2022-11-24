package io.axual.ksml.util;

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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringWriter;
import java.io.Writer;

import io.axual.ksml.exception.KSMLExecutionException;

public class JsonUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtil() {
    }

    public static String jsonToString(JsonNode node) {
        JsonFactory jsonFactory = new JsonFactory();
        Writer writer = new StringWriter();
        try {
            JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
            MAPPER.writeTree(jsonGenerator, node);
        } catch (Exception e) {
            throw new KSMLExecutionException("Could not write json as string");
        }
        return writer.toString();
    }
}
