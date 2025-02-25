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

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataValue;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.Symbol;
import io.axual.ksml.exception.ParseException;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

public class ChoiceParser<T> extends BaseParser<T> implements StructsParser<T>, NamedObjectParser {
    private final String childName;
    private final String parsedType;
    private final String defaultValue;
    private final Map<String, StructsParser<? extends T>> parsers;
    @Getter
    private final List<StructSchema> schemas = new ArrayList<>();

    public ChoiceParser(String childName, String enumType, String description, String defaultValue, Map<String, StructsParser<? extends T>> parsers) {
        this.childName = childName;
        this.parsedType = description;
        this.defaultValue = defaultValue;
        this.parsers = new HashMap<>(parsers);

        // To generate proper JSON Schema, first map all parseable schema to fixed values associated with them
        final Map<StructSchema, List<String>> schemaToChildValues = new HashMap<>();
        this.parsers.forEach((name, parser) -> {
            final var parserSchemas = parser.schemas();
            for (final var parserSchema : parserSchemas) {
                schemaToChildValues.putIfAbsent(parserSchema, new ArrayList<>());
                schemaToChildValues.get(parserSchema).add(name);
            }
        });

        // Now generate new schema by adding the parsed child attribute (ie. "type") as a parsed field
        Map<String, StructSchema> convertedSchema = new TreeMap<>();
        for (final var entry : schemaToChildValues.entrySet()) {
            final var schema = entry.getKey();
            final var doc = "The " + childName + " of the " + description;
            final var newFields = new ArrayList<>(schema.fields());
            // The "type" field is required only if there are multiple options, and the one in this schema is not the default
            final var isDefault = entry.getValue().size() == 1 && entry.getValue().getFirst().equals(defaultValue);
            final var required = schemaToChildValues.size() > 1 && !isDefault;
            // Add the "type" field to the list of fields for the converted schema
            final var enumSchema = new EnumSchema(schema.namespace(), enumType, doc, entry.getValue().stream().map(Symbol::new).toList(), null);
            final var field = new DataField(childName, enumSchema, doc, DataField.NO_TAG, required, defaultValue != null, defaultValue != null ? new DataValue(defaultValue) : null);
            newFields.add(field);
            // Create a converted schema, which includes the "type" field
            final var newSchema = new StructSchema(schema.namespace(), schema.name(), schema.doc(), newFields);
            // Put in a map to deduplicate by name
            convertedSchema.put(schema.name(), newSchema);
        }

        // Finally, copy all converted schema into a list of schemas that this parser handles
        schemas.addAll(convertedSchema.values());
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
    public void defaultShortName(String name) {
        parsers.values().forEach(p -> {
            if (p instanceof NamedObjectParser nop) nop.defaultShortName(name);
        });
    }

    @Override
    public void defaultLongName(String name) {
        parsers.values().forEach(p -> {
            if (p instanceof NamedObjectParser nop) nop.defaultLongName(name);
        });
    }
}
