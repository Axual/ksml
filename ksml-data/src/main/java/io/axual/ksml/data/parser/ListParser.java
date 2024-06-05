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

import java.util.ArrayList;
import java.util.List;

public class ListParser<V> implements Parser<List<V>> {
    private final Parser<V> valueParser;
    private final String childTagKey;
    private final String childTagValuePrefix;

    public ListParser(String childTagKey, String childTagValuePrefix, Parser<V> valueParser) {
        this.childTagKey = childTagKey;
        this.childTagValuePrefix = childTagValuePrefix;
        this.valueParser = valueParser;
    }

    @Override
    public List<V> parse(ParseNode node) {
        List<V> result = new ArrayList<>();
        if (node != null) {
            var index = 0;
            for (ParseNode childNode : node.children(childTagKey, childTagValuePrefix)) {
                try {
                    index++;
                    result.add(valueParser.parse(childNode));
                } catch (RuntimeException e) {
                    throw new ParseException(node, "Error in " + childTagValuePrefix + " entry " + index, e);
                }
            }
        }
        return result;
    }
}
