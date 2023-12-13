package io.axual.ksml.parser;

import io.axual.ksml.data.schema.DataSchema;

import java.util.function.Function;
import java.util.function.Supplier;

public interface ParserWithSchema<T> extends Parser<T> {
    DataSchema schema();

    static <T> ParserWithSchema<T> of(final Function<YamlNode, T> parseFunc, DataSchema schema) {
        return of(parseFunc, () -> schema);
    }

    static <T> ParserWithSchema<T> of(final Function<YamlNode, T> parseFunc, Supplier<DataSchema> getter) {
        return new ParserWithSchema<T>() {
            @Override
            public T parse(YamlNode node) {
                return parseFunc.apply(node);
            }

            @Override
            public DataSchema schema() {
                return getter.get();
            }
        };
    }
}
