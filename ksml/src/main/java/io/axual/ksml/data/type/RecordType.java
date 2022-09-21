package io.axual.ksml.data.type;

import java.util.Objects;

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.schema.RecordSchema;

public class RecordType extends MapType {
    private final RecordSchema schema;

    public RecordType() {
        this(null);
    }

    public RecordType(RecordSchema schema) {
        super(DataString.DATATYPE, DataType.UNKNOWN, schema);
        this.schema = schema;
    }

    public RecordSchema schema() {
        return schema;
    }

    @Override
    public String toString() {
        String name = schema.name();
        return "Record" + (name.length() > 0 ? "<" + name + ">" : "");
    }

    @Override
    public String schemaName() {
        return "Record" + (schema.name().length() > 0 ? "Of" + schema.name() : "");
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (other instanceof RecordType recordType) {
            return schema.equals(recordType.schema);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), schema);
    }
}
