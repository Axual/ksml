package io.axual.ksml.parser;

import io.axual.ksml.data.schema.StructSchema;
import org.apache.commons.collections4.ListUtils;

public class CombinedParser<T, S> extends DefinitionParser<CombinedParser.Combination<T, S>> {
    private final ParserWithSchema<T> parser1;
    private final ParserWithSchema<S> parser2;
    private final StructSchema schema;

    public record Combination<T, S>(T first, S second) {
    }

    public CombinedParser(StructParser<T> parser1, StructParser<S> parser2) {
        this.parser1 = parser1;
        this.parser2 = parser2;
        schema = structSchema((String) null, null, ListUtils.union(parser1.fields(), parser2.fields()));
    }

    @Override
    public StructParser<Combination<T, S>> parser() {
        return StructParser.of(node -> new Combination<>(parser1.parse(node), parser2.parse(node)), schema);
    }
}
