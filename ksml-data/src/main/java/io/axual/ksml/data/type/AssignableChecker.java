package io.axual.ksml.data.type;

import io.axual.ksml.data.validation.ValidationContext;
import io.axual.ksml.data.validation.ValidationResult;

import java.util.Objects;

public class AssignableChecker {
    public final ValidationResult isAssignableFrom(DataType a, DataType b) {
        return isAssignableFrom(a, b, new ValidationContext());
    }

    private ValidationResult isAssignableFrom(DataType a, DataType b, ValidationContext context) {
        Objects.requireNonNull(a, "No data type specified, this is a bug in KSML");
        Objects.requireNonNull(b, "No data type specified, this is a bug in KSML");
        if (a instanceof SimpleType simpleTypeA) {
            if (!(b instanceof SimpleType simpleTypeB)) return context.typeMismatch(simpleTypeA, b);
            return isAssignableFrom(simpleTypeA, simpleTypeB, context);
        }
        if (a instanceof ComplexType complexTypeA) {
            if (!(b instanceof ComplexType complexTypeB)) return context.typeMismatch(complexTypeA, b);
            return isAssignableFrom(complexTypeA, complexTypeB, context);
        }
        return context.addError("Unknown data type \"" + a + "\"");
    }

    private ValidationResult isAssignableFrom(SimpleType a, SimpleType b, ValidationContext context) {
        if (!a.containerClass().isAssignableFrom(b.containerClass())) return context.typeMismatch(a, b);
        return context.ok();
    }

    private ValidationResult isAssignableFrom(ComplexType a, ComplexType b, ValidationContext context) {
        if (!a.containerClass().isAssignableFrom(b.containerClass())) return context.typeMismatch(a, b);
        if (a.subTypeCount() != b.subTypeCount())
            return context.addError("Type \"" + context.thatType(b) + "\" has a different number of sub-types than \"" + context.thisType(a) + "\"");

        for (int i = 0; i < a.subTypeCount(); i++) {
            if (!isAssignableFrom(a.subType(i), b.subType(i), context).isOK()) return context;
        }
        return context.ok();
    }
}
