package io.axual.ksml.data.type.user;

import io.axual.ksml.data.type.base.TupleType;

public class UserTupleType extends ComplexUserType {
    public UserTupleType(String notation, UserType... subTypes) {
        super(new TupleType(convertTypes(subTypes)), notation, subTypes);
    }
}
