package io.axual.ksml.schema;

import java.util.Objects;

public class MapSchema extends DataSchema {
    private final DataSchema valueSchema;

    public MapSchema(DataSchema valueType) {
        super(Type.MAP);
        this.valueSchema = valueType;
    }

    public DataSchema valueType() {
        return valueSchema;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields, note: explicitly do not compare the doc field
        return (Objects.equals(valueSchema, ((ArraySchema) other).valueType()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valueSchema);
    }
}
