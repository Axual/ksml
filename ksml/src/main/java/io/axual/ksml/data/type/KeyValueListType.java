package io.axual.ksml.data.type;

public class KeyValueListType extends ListType {
    public KeyValueListType(DataType keyType, DataType valueType) {
        super(new KeyValueType(keyType, valueType));
    }

    public DataType keyType() {
        return ((KeyValueType) valueType()).keyType();
    }

    public DataType valueType() {
        return ((KeyValueType) valueType()).valueType();
    }
}
