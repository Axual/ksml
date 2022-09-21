package io.axual.ksml.schema;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RecordSchema extends NamedSchema {
    private final List<DataField> fields = new ArrayList<>();

    public RecordSchema(RecordSchema other) {
        this(other.namespace(), other.name(), other.doc(), other.fields);
    }

    public RecordSchema(String namespace, String name, String doc, List<DataField> fields) {
        super(Type.RECORD, namespace, name, doc);
        if (fields != null) {
            this.fields.addAll(fields);
        }
    }

    public int numFields() {
        return fields.size();
    }

    public DataField field(int index) {
        return fields.get(index);
    }

    public DataField field(String name) {
        for (DataField field : fields) {
            if (field.name().equals(name)) {
                return field;
            }
        }

        return null;
    }

    public List<DataField> fields() {
        return Lists.newCopyOnWriteArrayList(fields);
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields, note: explicitly do not compare the doc field
        return !fields.equals(((RecordSchema) other).fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }
}

