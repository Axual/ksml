package io.axual.ksml.schema.parser;

import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.YamlNode;
import io.axual.ksml.schema.DataField;

public class DataFieldOrderParser extends BaseParser<DataField.Order> {
    @Override
    public DataField.Order parse(YamlNode node) {
        if (node == null) return DataField.Order.ASCENDING;
        var order = node.asText();
        if (order != null) order = order.toUpperCase();
        try {
            return DataField.Order.valueOf(order);
        } catch (IllegalArgumentException e) {
            return DataField.Order.ASCENDING;
        }
    }
}
