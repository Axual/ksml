package io.axual.ksml.parser;

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


import io.axual.ksml.execution.FatalError;

import java.util.ArrayList;
import java.util.List;

public class ListParser<V> implements Parser<List<V>> {
    private final Parser<V> valueParser;
    private final String whatToParse;

    public ListParser(String whatToParse, Parser<V> valueParser) {
        this.valueParser = valueParser;
        this.whatToParse = whatToParse;
    }

    @Override
    public List<V> parse(YamlNode node) {
        List<V> result = new ArrayList<>();
        if (node != null) {
            var index = 0;
            for (YamlNode childNode : node.children()) {
                try {
                    index++;
                    result.add(valueParser.parse(childNode));
                } catch (RuntimeException e) {
                    throw FatalError.parseError(node, "Error in " + whatToParse + " entry " + index, e);
                }
            }
        }
        return result;
    }
}
