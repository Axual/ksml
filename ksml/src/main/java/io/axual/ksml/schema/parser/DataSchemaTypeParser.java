package io.axual.ksml.schema.parser;

import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaWriter;

public class DataSchemaTypeParser extends BaseParser<DataSchema.Type> {
    @Override
    public DataSchema.Type parse(YamlNode node) {
        if (node.isArray()) return DataSchema.Type.UNION;
        if (node.isObject()) {
            var subtype = parseText(node, SchemaWriter.DATASCHEMA_TYPE_FIELD);
            return parseType(node, subtype);
        }
        if (node.isText()) {
            return parseType(node, node.asText());
        }
        return canNotParse(node);
    }

    private DataSchema.Type parseType(YamlNode node, String type) {
        return switch (type) {
            case "null" -> DataSchema.Type.NULL;
            case "byte" -> DataSchema.Type.BYTE;
            case "short" -> DataSchema.Type.SHORT;
            case "int", "integer" -> DataSchema.Type.INTEGER;
            case "long" -> DataSchema.Type.LONG;
            case "double" -> DataSchema.Type.DOUBLE;
            case "float" -> DataSchema.Type.FLOAT;
            case "bytes" -> DataSchema.Type.BYTES;
            case "fixed" -> DataSchema.Type.FIXED;
            case "string" -> DataSchema.Type.STRING;
            case "enum" -> DataSchema.Type.ENUM;
            case "array", "list" -> DataSchema.Type.LIST;
            case "map" -> DataSchema.Type.MAP;
            case "record" -> DataSchema.Type.RECORD;
            default -> canNotParse(node);
        };
    }

    private DataSchema.Type canNotParse(YamlNode node) {
        throw new KSMLParseException("Can not parse schema type: " + node);
    }
}
