package io.axual.ksml.data.parser.schema;

import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.type.SymbolMetadata;

public class SymbolMetadataParser extends BaseParser<SymbolMetadata> {
    @Override
    public SymbolMetadata parse(ParseNode node) {
        return new SymbolMetadata(
                parseString(node, DataSchemaDSL.ENUM_SCHEMA_DOC_FIELD),
                parseInteger(node, DataSchemaDSL.ENUM_SCHEMA_INDEX_FIELD));
    }
}
