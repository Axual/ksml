package io.axual.ksml.util;

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.parser.StructsParser;

import java.util.ArrayList;

public class ParserUtil {
    private ParserUtil() {
    }

    public static <V> StructsParser<V> optional(StructsParser<V> parser) {
        final var newSchemas = new ArrayList<StructSchema>();
        for (final var schema : parser.schemas()) {
            if (!schema.fields().isEmpty()) {
                final var newFields = schema.fields().stream()
                        .map(field -> field.required() ? new DataField(field.name(), field.schema(), "*(optional)* " + field.doc(), field.tag(), false, field.constant(), field.defaultValue()) : field)
                        .toList();
                newSchemas.add(new StructSchema(schema.namespace(), schema.name(), schema.doc(), newFields));
            } else {
                newSchemas.add(schema);
            }
        }
        return StructsParser.of(node -> node != null ? parser.parse(node) : null, newSchemas);
    }
}
