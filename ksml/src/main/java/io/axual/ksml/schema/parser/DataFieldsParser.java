package io.axual.ksml.schema.parser;

import java.util.List;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataField;
import io.axual.ksml.schema.SchemaWriter;

public class DataFieldsParser extends BaseParser<List<DataField>> {
    @Override
    public List<DataField> parse(YamlNode node) {
        return new ListParser<>(new DataFieldParser()).parse(node.get(SchemaWriter.RECORDSCHEMA_FIELDS_FIELD, "field"));
    }
}
