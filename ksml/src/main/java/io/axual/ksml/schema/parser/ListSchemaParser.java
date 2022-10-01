package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.ListSchema;
import io.axual.ksml.schema.SchemaWriter;

public class ListSchemaParser extends BaseParser<ListSchema> {
    @Override
    public ListSchema parse(YamlNode node) {
        return new ListSchema(new DataSchemaParser().parse(node.get(SchemaWriter.LISTSCHEMA_VALUES_FIELD)));
    }
}
