package io.axual.ksml.data.type.user;

import io.axual.ksml.data.type.base.DataType;

public class StaticUserType implements UserType {
    private final DataType type;
    private final String notation;

    public StaticUserType(DataType type, String notation) {
        this.type = type;
        this.notation = notation;
    }

    public DataType type() {
        return type;
    }

    public String notation() {
        return notation;
    }
}
