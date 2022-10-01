package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.FixedSchema;
import io.axual.ksml.schema.SchemaWriter;

public class FixedSchemaParser extends BaseParser<FixedSchema> {
    @Override
    public FixedSchema parse(YamlNode node) {
        return new FixedSchema(
                parseText(node,SchemaWriter.NAMEDSCHEMA_NAMESPACE_FIELD),
                parseText(node,SchemaWriter.NAMEDSCHEMA_NAME_FIELD),
                parseText(node,SchemaWriter.NAMEDSCHEMA_DOC_FIELD),
                parseInteger(node,SchemaWriter.FIXEDSCHEMA_SIZE_FIELD));
    }
}
