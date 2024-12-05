package io.axual.ksml.data.type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.axual.ksml.data.type.SymbolMetadata.NO_INDEX;

public class Symbols extends LinkedHashMap<String, SymbolMetadata> {
    private static final SymbolMetadata DEFAULT_METADATA = new SymbolMetadata(null, NO_INDEX);

    public Symbols() {
    }

    public Symbols(Map<String, SymbolMetadata> map) {
        putAll(map);
    }

    public static Symbols from(List<String> symbols) {
        final var result = new Symbols();
        for (final var symbol : symbols) result.put(symbol, DEFAULT_METADATA);
        return result;
    }
}
