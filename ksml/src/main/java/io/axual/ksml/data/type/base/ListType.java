package io.axual.ksml.data.type.base;

import java.util.List;

public class ListType extends ComplexType {
    public ListType(DataType valueType) {
        super(List.class, valueType);
    }

    public String schemaName() {
        return "ListOf" + valueType().schemaName();
    }

    public DataType valueType() {
        return subType(0);
    }
}
