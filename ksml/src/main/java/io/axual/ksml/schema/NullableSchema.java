package io.axual.ksml.schema;

import java.util.Objects;

public class NullableSchema extends DataSchema {
    private final DataSchema valueType;

    public NullableSchema( DataSchema valueType) {
        super(Type.NULLABLE);
        this.valueType = valueType;
    }

    public DataSchema valueType() {
        return valueType;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields, note: explicitly do not compare the doc field
        return Objects.equals(valueType, ((NullableSchema) other).valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valueType);
    }
}
