package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ListParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.EnumSchema;
import io.axual.ksml.schema.SchemaWriter;

public class EnumSchemaParser extends BaseParser<EnumSchema> {
    @Override
    public EnumSchema parse(YamlNode node) {
        return new EnumSchema(
                parseText(node, SchemaWriter.NAMEDSCHEMA_NAMESPACE_FIELD),
                parseText(node, SchemaWriter.NAMEDSCHEMA_NAME_FIELD),
                parseText(node, SchemaWriter.NAMEDSCHEMA_DOC_FIELD),
                new ListParser<>(new SymbolParser()).parse(node.get(SchemaWriter.ENUMSCHEMA_POSSIBLEVALUES_FIELD)),
                parseText(node, SchemaWriter.ENUMSCHEMA_DEFAULTVALUE_FIELD));
    }
}
