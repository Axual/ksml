package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;

public class SymbolParser extends BaseParser<String> {
    @Override
    public String parse(YamlNode node) {
        return node.asText();
    }
}
