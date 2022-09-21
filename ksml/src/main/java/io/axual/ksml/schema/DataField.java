package io.axual.ksml.schema;

import io.axual.ksml.data.object.DataObject;

public record DataField(String name, DataSchema schema,
                        String doc, DataObject defaultValue,
                        Order order) {
    public enum Order {
        ASCENDING,
        DESCENDING,
        IGNORE;
    }
}
