package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.RecordSchema;

public class RecordSchemaParser extends BaseParser<RecordSchema> {
    @Override
    public RecordSchema parse(YamlNode node) {
        var namespace = node.get("namespace").asText();
        var name = node.get("name").asText();
        var doc = node.get("doc").asText();
        var fields = new DataFieldsParser().parse(node);
        return new RecordSchema(namespace, name, doc, fields);
    }
}
