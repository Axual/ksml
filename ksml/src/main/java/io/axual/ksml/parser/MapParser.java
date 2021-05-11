package io.axual.ksml.parser;

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



import java.util.HashMap;
import java.util.Map;

public class MapParser<V> extends BaseParser<Map<String, V>> {
    private final BaseParser<V> valueParser;

    public MapParser(BaseParser<V> valueParser) {
        this.valueParser = valueParser;
    }

    @Override
    public Map<String, V> parse(YamlNode node) {
        Map<String, V> result = new HashMap<>();
        if (node != null) {
            for (YamlNode child : node.getChildren()) {
                result.put(child.getName(), (valueParser.parse(child)));
            }
        }
        return result;
    }
}
