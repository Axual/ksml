package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataSchema;
import io.axual.ksml.schema.UnionSchema;

public class UnionSchemaParser extends BaseParser<UnionSchema> {
    @Override
    public UnionSchema parse(YamlNode node) {
        var possibleTypes = new ListParser<>(new DataSchemaParser()).parse(node);
        return new UnionSchema(possibleTypes.toArray(new DataSchema[0]));
    }
}
