package io.axual.ksml.parser;

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.value.Pair;
import org.apache.commons.collections4.ListUtils;

public class CombinedParser<L, R> extends DefinitionParser<Pair<L, R>> {
    private final ParserWithSchema<L> parser1;
    private final ParserWithSchema<R> parser2;
    private final StructSchema schema;

    public CombinedParser(StructParser<L> parser1, StructParser<R> parser2) {
        this.parser1 = parser1;
        this.parser2 = parser2;
        schema = structSchema((String) null, null, ListUtils.union(parser1.fields(), parser2.fields()));
    }

    @Override
    public StructParser<Pair<L, R>> parser() {
        return StructParser.of(node -> Pair.of(parser1.parse(node), parser2.parse(node)), schema);
    }
}
