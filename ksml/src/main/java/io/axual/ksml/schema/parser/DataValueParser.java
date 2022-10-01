package io.axual.ksml.schema.parser;

import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataValue;

public class DataValueParser extends BaseParser<DataValue> {
    @Override
    public DataValue parse(YamlNode node) {
        if (node == null) return null;
        if (node.isNull()) return new DataValue(null);
        if (node.isBoolean()) return new DataValue(node.asBoolean());
        if (node.isInt()) return new DataValue(node.asInt());
        if (node.isLong()) return new DataValue(node.asLong());
        if (node.isDouble()) return new DataValue(node.asDouble());
        if (node.isText()) return new DataValue(node.asText());
        throw new KSMLParseException("Can not parse value type: " + node.asText());
    }
}
