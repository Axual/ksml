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


import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.user.UserFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

@Slf4j
public class BaseOperation implements StreamOperation {
    private static final String ERROR_IN_TOPOLOGY = "Error in topology";
    private static final String[] TEMPLATE = new String[0];

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
    protected final String[] storeNames;

    public BaseOperation(OperationConfig config) {
        var error = NameValidator.validateNameAndReturnError(config.name);
        if (error != null) {
            log.warn("Ignoring name with error '" + config.name + "': " + error);
            name = null;
        } else {
            name = config.name;
        }
        notationLibrary = config.notationLibrary;
        storeNames = config.storeNames;
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

    private UserType[] arrayFrom(UserType element, UserType[] elements) {
        final var result = new UserType[elements.length + 1];
        result[0] = element;
        System.arraycopy(elements, 0, result, 1, elements.length);
        return result;
    }

    private UserType[] toUserTypes(StreamDataType[] streamDataTypes) {
        return Arrays.stream(streamDataTypes).map(StreamDataType::userType).toArray(UserType[]::new);
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, UNKNOWN is returned.
    protected UserType firstSpecificType(UserFunction func1, UserFunction func2, UserFunction func3) {
        return firstSpecificType(func1, func2, func3, UserType.UNKNOWN);
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of stream data types.
    protected UserType firstSpecificType(UserFunction func1, UserFunction func2, UserFunction func3, StreamDataType... streamDataTypes) {
        return firstSpecificType(func1, func2, func3, toUserTypes(streamDataTypes));
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, UNKNOWN is returned.
    protected UserType firstSpecificType(UserFunction func1, UserFunction func2) {
        return firstSpecificType(func1, func2, UserType.UNKNOWN);
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of user types.
    protected UserType firstSpecificType(UserFunction func1, UserFunction func2, UserFunction func3, UserType... userTypes) {
        return firstSpecificType(func1, func2, arrayFrom(func3.resultType, userTypes));
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of stream data types.
    protected UserType firstSpecificType(UserFunction func1, UserFunction func2, StreamDataType... streamDataTypes) {
        return firstSpecificType(func1, func2, toUserTypes(streamDataTypes));
    }

    protected UserType firstSpecificType(UserFunction func1, UserFunction func2, UserType... userTypes) {
        return firstSpecificType(func1, arrayFrom(func2.resultType, userTypes));
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of stream data types.
    protected UserType firstSpecificType(UserFunction function, StreamDataType... streamDataTypes) {
        return firstSpecificType(function, toUserTypes(streamDataTypes));
    }

    protected UserType firstSpecificType(UserFunction function, UserType... userTypes) {
        return firstSpecificType(arrayFrom(function.resultType, userTypes));
    }

    // Returns the most specific type in the sequence by traversing the array and checking for DataType.UNKNOWNs. The
    // result is the first non-UNKNOWN, or otherwise the last entry in the array.
    private UserType firstSpecificType(UserType... types) {
        for (int index = 0; index < types.length - 1; index++) {
            if (types[index].dataType() != DataType.UNKNOWN) return types[index];
        }
        return types[types.length - 1];
    }

    protected TypeComparator equalTo(StreamDataType compareType) {
        return equalTo(compareType.userType());
    }

    protected TypeComparator equalTo(UserType compareType) {
        return equalTo(compareType.dataType());
    }

    protected TypeComparator equalTo(DataType compareType) {
        return new TypeComparator(
                type -> compareType.isAssignableFrom(type) && type.isAssignableFrom(compareType),
                "of type " + compareType);
    }

    protected TypeComparator superOf(StreamDataType compareType) {
        return superOf(compareType.userType());
    }

    protected TypeComparator superOf(UserType compareType) {
        return superOf(compareType.dataType());
    }

    protected TypeComparator superOf(DataType compareType) {
        return new TypeComparator(
                type -> type.isAssignableFrom(compareType),
                "(superclass of) type " + compareType);
    }

    protected TypeComparator subOf(StreamDataType compareType) {
        return subOf(compareType.userType());
    }

    protected TypeComparator subOf(UserType compareType) {
        return subOf(compareType.dataType());
    }

    protected TypeComparator subOf(DataType compareType) {
        return new TypeComparator(
                compareType::isAssignableFrom,
                "(subclass of) type " + compareType);
    }

    protected void checkType(String subject, StreamDataType type, TypeComparator comparator) {
        checkType(subject, type.userType(), comparator);
    }

    protected void checkType(String subject, UserType type, TypeComparator comparator) {
        checkType(subject, type.dataType(), comparator);
    }

    protected void checkType(String subject, DataType type, TypeComparator comparator) {
        if (!comparator.checker.compare(type)) {
            throw topologyError(subject + " is expected to be " + comparator.faultDescription);
        }
    }

    protected void checkFunction(String functionType, UserFunction function, StreamDataType appliedResultType, TypeComparator... parameters) {
        checkFunction(functionType, function, superOf(appliedResultType), appliedResultType, parameters);
    }

    protected void checkFunction(String functionType, UserFunction function, TypeComparator resultType, StreamDataType appliedResultType, TypeComparator... parameters) {
        checkFunction(functionType, function, resultType, appliedResultType.userType(), parameters);
    }

    protected void checkFunction(String functionType, UserFunction function, UserType appliedResultType, TypeComparator... parameters) {
        checkFunction(functionType, function, superOf(appliedResultType), appliedResultType, parameters);
    }

    protected void checkFunction(String functionType, UserFunction function, TypeComparator resultType, UserType appliedResultType, TypeComparator... parameters) {
        // Check if the function is defined
        if (function == null) {
            throw topologyError(functionType + " is not defined");
        }

        // Check if the resultType of the function is as expected
        checkType(functionType + " resultType", (function.resultType != null ? function.resultType.dataType() : DataNull.DATATYPE), resultType);
        // Update the applied result type of the function
        function.appliedResultType = appliedResultType;

        // Check if the number of parameters is as expected
        if (function.fixedParameterCount > parameters.length) {
            throw topologyError(functionType + " is expected to take at least " + function.fixedParameterCount + " parameters");
        }
        if (function.parameters.length < parameters.length) {
            throw topologyError(functionType + " is expected to take at most " + function.parameters.length + " parameters");
        }

        // Check if all parameters are of expected type
        for (int index = 0; index < parameters.length; index++) {
            checkType(functionType + " parameter " + (index + 1) + " (\"" + function.parameters[index].name() + "\")", function.parameters[index].type(), parameters[index]);
        }
    }

    protected void checkTuple(String faultDescription, UserType type, DataType... elements) {
        checkTuple(faultDescription, type.dataType(), elements);
    }

    protected void checkTuple(String faultDescription, DataType type, DataType... elements) {
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
    }

    protected StreamDataType streamDataTypeOf(String notationName, DataType dataType, boolean isKey) {
        return streamDataTypeOf(new UserType(notationName, dataType), isKey);
    }

    protected StreamDataType streamDataTypeOf(UserType userType, boolean isKey) {
        return new StreamDataType(notationLibrary, userType, isKey);
    }

    protected String[] combineStoreNames(String[]... storeNameArrays) {
        final var storeNames = new TreeSet<String>();
        for (String[] storeNameArray : storeNameArrays) {
            if (storeNameArray != null) Collections.addAll(storeNames, storeNameArray);
        }
        return storeNames.toArray(TEMPLATE);
    }
}
