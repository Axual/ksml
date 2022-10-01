package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.MapSchema;
import io.axual.ksml.schema.SchemaWriter;

public class MapSchemaParser extends BaseParser<MapSchema> {
    @Override
    public MapSchema parse(YamlNode node) {
        return new MapSchema(new DataSchemaParser().parse(node.get(SchemaWriter.MAPSCHEMA_VALUES_FIELD)));
    }
}
