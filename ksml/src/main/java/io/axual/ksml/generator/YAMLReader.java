package io.axual.ksml.generator;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class YAMLReader {
    public static List<YAMLDefinition> readYAML(ObjectMapper mapper, String baseDir, Object source) throws IOException {
        if (source instanceof String sourceString) {
            String fullSourcePath = Path.of(baseDir, sourceString).toString();
            return Collections.singletonList(new YAMLDefinition(fullSourcePath, mapper.readValue(new File(fullSourcePath), JsonNode.class)));
        }
        if (source instanceof Collection<?> sourceCollection) {
            List<YAMLDefinition> result = new ArrayList<>();
            for (Object element : sourceCollection) {
                result.addAll(readYAML(mapper, baseDir, element));
            }
            return result;
        }
        return new ArrayList<>();
    }
}
