package io.axual.ksml.parser;

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.MapSchema;
import lombok.Getter;

import java.util.Map;

@Getter
public class MapWithSchemaParser<T> extends MapParser<T> implements ParserWithSchema<Map<String, T>> {
    private final DataSchema schema;

    public MapWithSchemaParser(String whatToParse, ParserWithSchema<T> valueParser) {
        super(whatToParse, valueParser);
        schema = new MapSchema(valueParser.schema());
    }
}
