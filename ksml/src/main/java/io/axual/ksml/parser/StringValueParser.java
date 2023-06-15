package io.axual.ksml.parser;

public class StringValueParser extends BaseParser<String> {
    @Override
    public String parse(YamlNode node) {
        return parseStringValue(node);
    }
}
