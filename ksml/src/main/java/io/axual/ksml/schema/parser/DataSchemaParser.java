package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.SchemaWriter;

public class DataSchemaParser extends BaseParser<DataSchema> {
    @Override
    public DataSchema parse(YamlNode node) {
        var typeNode = node.isObject() ? node.get(SchemaWriter.DATASCHEMA_TYPE_FIELD) : node;
        var parseNode = typeNode.isObject() || typeNode.isArray() ? typeNode : node;
        var schemaType = new DataSchemaTypeParser().parse(typeNode);
        return switch (schemaType) {
            case NULL, BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BYTES, STRING -> DataSchema.create(schemaType);
            case FIXED -> new FixedSchemaParser().parse(parseNode);
            case ENUM -> new EnumSchemaParser().parse(parseNode);
            case LIST -> new ListSchemaParser().parse(parseNode);
            case MAP -> new MapSchemaParser().parse(parseNode);
            case RECORD -> new RecordSchemaParser().parse(parseNode);
            case UNION -> new UnionSchemaParser().parse(parseNode);
        };
    }
}
