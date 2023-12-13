package io.axual.ksml.parser;

import com.google.common.collect.ImmutableMap;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.execution.FatalError;
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
    public T parse(YamlNode node) {
        if (node == null) return null;
        final var child = node.get(childName);
        if (child == null) return null;
        String childValue = child.asString();
        childValue = childValue != null ? childValue : defaultValue;
        if (!parsers.containsKey(childValue)) {
            throw FatalError.parseError(child, "Unknown " + parsedType + " \"" + childName + "\", choose one of " + parsers.keySet().stream().sorted().collect(Collectors.joining(", ")));
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
