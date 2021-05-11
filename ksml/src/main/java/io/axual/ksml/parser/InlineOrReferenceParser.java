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



import java.util.Map;

public class InlineOrReferenceParser<T, F extends T> extends BaseParser<T> {
    private final Map<String, T> library;
    private final BaseParser<F> inlineParser;
    private final String childName;

    public InlineOrReferenceParser(Map<String, T> library, BaseParser<F> inlineParser, String childName) {
        this.library = library;
        this.inlineParser = inlineParser;
        this.childName = childName;
    }

    @Override
    public T parse(YamlNode node) {
        if (node == null) return null;
        if (node.childIsText(childName)) {
            return library.get(parseText(node, childName));
        }
        return inlineParser.parse(node.get(childName));
    }
}
