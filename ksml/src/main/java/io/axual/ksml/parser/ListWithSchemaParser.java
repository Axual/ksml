package io.axual.ksml.parser;

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.ListSchema;
import lombok.Getter;

import java.util.List;

@Getter
public class ListWithSchemaParser<T> extends ListParser<T> implements ParserWithSchema<List<T>> {
    private final DataSchema schema;

    public ListWithSchemaParser(String whatToParse, ParserWithSchema<T> valueParser) {
        super(whatToParse, valueParser);
        schema = new ListSchema(valueParser.schema());
    }
}
