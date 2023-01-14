package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */


import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Named;

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.parser.StreamOperation;
import io.axual.ksml.user.UserFunction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseOperation implements StreamOperation {
    private static final String ERROR_IN_TOPOLOGY = "Error in topology";

    private static class NameValidator extends Named {
        // Satisfy compiler with dummy constructor
        private NameValidator() {
            super("nonsense");
        }

        // Define a static method that calls the protected validate method in Named
        public static String validateNameAndReturnError(String name) {
            try {
                Named.validate(name);
                return null;
            } catch (TopologyException e) {
                return e.getMessage();
            }
        }
    }

    protected final String name;
    protected final NotationLibrary notationLibrary;

    public BaseOperation(OperationConfig config) {
        var error = NameValidator.validateNameAndReturnError(config.name);
        if (error != null) {
            log.warn("Ignoring name with error '" + config.name + "': " + error);
            name = null;
        } else {
            name = config.name;
        }
        notationLibrary = config.notationLibrary;
    }

    @Override
    public String toString() {
        var operation = getClass().getSimpleName();
        if (operation.toLowerCase().endsWith("operation")) {
            operation = operation.substring(0, operation.length() - 9);
        }
        return (name == null ? "Unnamed" : name) + " operation " + operation;
    }

    protected void checkNotNull(Object object, String description) {
        if (object == null) {
            throw new KSMLTopologyException(ERROR_IN_TOPOLOGY + ": " + description + " not defined");
        }
    }

    protected interface TypeCompatibilityChecker {
        boolean compare(DataType type);
    }

    protected record TypeComparator(TypeCompatibilityChecker checker, String faultDescription) {
    }

    protected KSMLTopologyException topologyError(String message) {
        return new KSMLTopologyException(ERROR_IN_TOPOLOGY + ": " + message);
    }

    protected TypeComparator equalTo(DataType compareType) {
        return new TypeComparator(
                type -> compareType.isAssignableFrom(type) && type.isAssignableFrom(compareType),
                "of type " + compareType);
    }

    protected TypeComparator superOf(DataType compareType) {
        return new TypeComparator(
                type -> type.isAssignableFrom(compareType),
                "(superclass of) type " + compareType);
    }

    protected TypeComparator subOf(DataType compareType) {
        return new TypeComparator(
                compareType::isAssignableFrom,
                "(subclass of) type " + compareType);
    }

    protected void checkType(String subject, DataType type, TypeComparator comparator) {
        if (!comparator.checker.compare(type)) {
            throw topologyError(subject + " is expected to be " + comparator.faultDescription);
        }
    }

    protected void checkFunction(String functionType, UserFunction function, TypeComparator resultType, TypeComparator... parameters) {
        // Check if the function is defined
        if (function == null) {
            throw topologyError(functionType + " is not defined");
        }

        // Check if the resultType of the function is as expected
        checkType(functionType + " resultType", (function.resultType != null ? function.resultType.dataType() : DataNull.DATATYPE), resultType);

        // Check if the number of parameters is as expected
        if (function.parameters.length != parameters.length) {
            throw topologyError(functionType + " is expected to take " + parameters.length + " parameters");
        }

        // Check if all parameters are of expected type
        for (int index = 0; index < function.parameters.length; index++) {
            checkType(functionType + " parameter " + (index + 1) + " (\"" + function.parameters[index].name() + "\")", function.parameters[index].type(), parameters[index]);
        }
    }

    protected TupleType checkTuple(String faultDescription, DataType type, DataType... elements) {
        if (!(type instanceof TupleType tupleType)) {
            throw new KSMLTopologyException(ERROR_IN_TOPOLOGY + ": " + faultDescription + " is expected to be a tuple");
        }
        if (tupleType.subTypeCount() != elements.length) {
            throw new KSMLTopologyException(ERROR_IN_TOPOLOGY + ": " + faultDescription + " is expected to be a tuple with " + elements.length + " elements");
        }
        for (int index = 0; index < elements.length; index++) {
            if (!elements[index].isAssignableFrom(tupleType.subType(index))) {
                throw new KSMLTopologyException(ERROR_IN_TOPOLOGY + ": " + faultDescription + " tuple element " + index + " is expected to be (subclass) of type " + elements[index]);
            }
        }
        return tupleType;
    }

    protected void checkAssignable(DataType superType, DataType subType, String message) {
        if (!superType.isAssignableFrom(subType)) {
            throw new KSMLTopologyException(ERROR_IN_TOPOLOGY + ": " + message + " (" + superType + " <--> " + subType + ")");
        }
    }

    protected void checkEqual(DataType type1, DataType type2, String message) {
        if (!type1.isAssignableFrom(type2) || !type2.isAssignableFrom(type1)) {
            throw new KSMLTopologyException(ERROR_IN_TOPOLOGY + ": " + message + " (" + type1 + " <--> " + type2 + ")");
        }
    }

    protected StreamDataType streamDataTypeOf(String notationName, DataType dataType, boolean isKey) {
        return new StreamDataType(notationLibrary, new UserType(notationName, dataType), isKey);
    }

    protected StreamDataType streamDataTypeOf(UserType userType, boolean isKey) {
        return new StreamDataType(notationLibrary, userType, isKey);
    }
}
