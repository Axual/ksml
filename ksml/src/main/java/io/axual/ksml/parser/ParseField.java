package io.axual.ksml.parser;

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataValue;
import lombok.Getter;

@Getter
public class ParseField<T> extends DataField {
    public interface Parser<T> {
        T parse(YamlNode node);
    }

    private Parser<T> parser;
    private boolean mandatory;

    public ParseField(String name, boolean mandatory, DataSchema schema, String doc, DataValue defaultValue, Order order, Parser<T> parser) {
        super(name, schema, doc, defaultValue, order);
        this.parser = parser;
        this.mandatory = mandatory;
    }

    public ParseField(String name, boolean mandatory, DataSchema schema, String doc, DataValue defaultValue, Parser<T> parser) {
        super(name, schema, doc, defaultValue);
        this.parser = parser;
        this.mandatory = mandatory;
    }

    public ParseField(String name, boolean mandatory, DataSchema schema, String doc, Parser<T> parser) {
        super(name, schema, doc);
        this.parser = parser;
        this.mandatory = mandatory;
    }
}
