package io.axual.ksml.data.parser;

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


import io.axual.ksml.data.exception.ParseException;

import java.util.LinkedHashMap;
import java.util.Map;

public class MapParser<V> implements Parser<Map<String, V>> {
    private final String childTagKey;
    private final String childTagValuePrefix;
    private final Parser<V> valueParser;

    public MapParser(String childTagKey, String childTagValuePrefix, Parser<V> valueParser) {
        this.childTagKey = childTagKey;
        this.childTagValuePrefix = childTagValuePrefix;
        this.valueParser = valueParser;
    }

    @Override
    public Map<String, V> parse(ParseNode node) {
        // Parse into a LinkedHashMap to preserve insertion order
        Map<String, V> result = new LinkedHashMap<>();
        if (node != null) {
            for (ParseNode child : node.children(childTagKey, "")) {
                try {
                    if (valueParser instanceof NamedObjectParser nop) {
                        nop.defaultShortName(child.name());
                        nop.defaultLongName(child.longName());
                    }
                    result.put(child.name(), valueParser.parse(child));
                } catch (RuntimeException e) {
                    throw new ParseException(node, "Error in " + childTagValuePrefix + " \"" + child.name() + "\"", e);
                }
            }
        }
        return result;
    }
}
