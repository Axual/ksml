package io.axual.ksml.schema;

import java.util.Objects;

public class ArraySchema extends DataSchema {
    private final DataSchema valueSchema;

    public ArraySchema(DataSchema valueType) {
        super(Type.ARRAY);
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

        // Compare all schema relevant fields
        return (Objects.equals(valueSchema, ((ArraySchema) other).valueSchema));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valueSchema);
    }
}
