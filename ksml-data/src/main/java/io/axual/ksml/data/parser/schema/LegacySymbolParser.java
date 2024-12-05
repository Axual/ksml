package io.axual.ksml.data.parser.schema;

import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ParseNode;

public class LegacySymbolParser extends BaseParser<String> {
    @Override
    public String parse(ParseNode node) {
        return node.asString();
    }
}
