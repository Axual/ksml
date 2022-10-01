package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.SchemaWriter;

public class DataFieldParser extends BaseParser<DataField> {
    @Override
    public DataField parse(YamlNode node) {
        return new DataField(
                parseText(node, SchemaWriter.DATAFIELD_NAME_FIELD),
                new DataSchemaParser().parse(node),
                parseText(node, SchemaWriter.DATAFIELD_DOC_FIELD),
                new DataValueParser().parse(node.get(SchemaWriter.DATAFIELD_DEFAULT_VALUE_FIELD)),
                new DataFieldOrderParser().parse(node.get(SchemaWriter.DATAFIELD_ORDER_FIELD)));
    }
}
