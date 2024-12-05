package io.axual.ksml.data.parser.schema;

import io.axual.ksml.data.exception.ParseException;
import io.axual.ksml.data.parser.BaseParser;
import io.axual.ksml.data.parser.ListParser;
import io.axual.ksml.data.parser.MapParser;
import io.axual.ksml.data.parser.ParseNode;
import io.axual.ksml.data.type.Symbols;

public class SymbolsParser extends BaseParser<Symbols> {
    public Symbols parse(ParseNode node) {
        // If the node is a JSON object, then parse the symbols using the "map of string to symbol metadata" method
        if (node.isObject()) {
            final var symbols = new MapParser<>("enum-symbol", "symbol", new SymbolMetadataParser()).parse(node);
            return new Symbols(symbols);
        }

        // If the node is an array, then parse the symbols by using the "list of strings" method (KSML 1.0.x and older)
        if (node.isArray()) {
            final var symbols = new ListParser<>("enum-symbol", "symbol", new LegacySymbolParser()).parse(node);
            return Symbols.from(symbols);
        }

        throw new ParseException(node, "Could not parse enum symbols properly");
    }
}
