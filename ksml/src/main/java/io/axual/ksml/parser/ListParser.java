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



import java.util.ArrayList;
import java.util.List;

public class ListParser<V> extends BaseParser<List<V>> {
    private final BaseParser<V> valueParser;
    private final int listStartIndex;

    public ListParser(BaseParser<V> valueParser, int listStartIndex) {
        this.valueParser = valueParser;
        this.listStartIndex = listStartIndex;
    }

    @Override
    public List<V> parse(YamlNode node) {
        List<V> result = new ArrayList<>();
        if (node != null) {
            for (YamlNode childNode : node.getChildren(listStartIndex)) {
                result.add(valueParser.parse(childNode));
            }
        }
        return result;
    }
}
