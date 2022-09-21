package io.axual.ksml.schema;

import java.util.Objects;

public class NamedSchema extends DataSchema {
    private final String namespace;
    private final String name;
    private final String doc;

    public NamedSchema(Type type, String namespace, String name, String doc) {
        super(type);
        this.namespace = namespace;
        if (name == null || name.length() == 0) {
            name = "Unnamed" + getClass().getSimpleName();
        }
        this.name = name;
        this.doc = doc;
    }

    public String namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    public String doc() {
        return doc;
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields
        if (!Objects.equals(namespace, ((NamedSchema) other).namespace)) return false;
        if (!Objects.equals(name, ((NamedSchema) other).name)) return false;
        return Objects.equals(doc, ((NamedSchema) other).doc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), namespace, name, doc);
    }
}
