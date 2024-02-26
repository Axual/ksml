package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import com.google.common.collect.ImmutableMap;
import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.NamedObjectParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.parser.ParserWithSchema;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.UnionSchema;
import lombok.Getter;

import java.util.Map;
import java.util.stream.Collectors;

public class ChoiceParser<T> extends BaseParser<T> implements ParserWithSchema<T>, NamedObjectParser {
    private final String childName;
    private final String parsedType;
    private final String defaultValue;
    private final Map<String, StructParser<? extends T>> parsers;
    @Getter
    private final DataSchema schema;

    public ChoiceParser(String childName, String parsedType, String defaultValue, Map<String, StructParser<? extends T>> parsers) {
        this.childName = childName;
        this.parsedType = parsedType;
        this.defaultValue = defaultValue;
        this.parsers = ImmutableMap.copyOf(parsers);
        if (parsers.size() == 1) {
            this.schema = parsers.values().iterator().next().schema();
        } else {
            this.schema = new UnionSchema(parsers.values().stream().map(StructParser::schema).toArray(DataSchema[]::new));
        }
    }

    @Override
    public T parse(ParseNode node) {
        if (node == null) return null;
        final var child = node.get(childName);
        String childValue = defaultValue;
        if (child != null) {
            childValue = child.asString();
            childValue = childValue != null ? childValue : defaultValue;
        }
        if (!parsers.containsKey(childValue)) {
            throw new ParseException(child, "Unknown " + parsedType + " \"" + childName + "\", choose one of " + parsers.keySet().stream().sorted().collect(Collectors.joining(", ")));
        }
        return parsers.get(childValue).parse(node);
    }

    @Override
    public void defaultName(String name) {
        parsers.values().forEach(p -> {
            if (p instanceof NamedObjectParser nop) nop.defaultName(name);
        });
    }
}
