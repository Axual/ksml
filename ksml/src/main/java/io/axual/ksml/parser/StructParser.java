package io.axual.ksml.parser;

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.StructSchema;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public interface StructParser<T> extends ParserWithSchema<T> {
    StructSchema schema();

    default List<DataField> fields() {
        return schema().fields();
    }

    static <T> StructParser<T> of(final Function<YamlNode, T> parseFunc, StructSchema schema) {
        return of(parseFunc, () -> schema);
    }

    static <T> StructParser<T> of(final Function<YamlNode, T> parseFunc, Supplier<StructSchema> getter) {
        return new StructParser<T>() {
            @Override
            public T parse(YamlNode node) {
                return parseFunc.apply(node);
            }

            @Override
            public StructSchema schema() {
                return getter.get();
            }
        };
    }
}
