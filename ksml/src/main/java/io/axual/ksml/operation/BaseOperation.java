package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;

import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.user.UserFunction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseOperation implements StreamOperation {
    private static final String ERROR_IN_TOPOLOGY = "Error in topology";
    protected static final String[] TEMPLATE = new String[0];

    private static class NameValidator extends Named {
        // Satisfy compiler with dummy constructor
        private NameValidator() {
            super("nonsense");
        }

        // Define a static method that calls the protected validate method in Named
        public static String validateNameAndReturnError(String name) {
            try {
                if (name != null) Named.validate(name);
                return null;
            } catch (org.apache.kafka.streams.errors.TopologyException e) {
                return e.getMessage();
            }
        }
    }

    protected final String name;
    protected final ContextTags tags;
    protected final String[] storeNames;

    public BaseOperation(OperationConfig config) {
        var error = NameValidator.validateNameAndReturnError(config.name());
        if (error != null) {
            log.warn("Ignoring name with error '" + config.name() + "': " + error);
            name = null;
        } else {
            name = config.name();
        }
        tags = config.tags().append("operation-name", name);
        storeNames = config.storeNames() != null ? config.storeNames() : new String[0];
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
            throw new TopologyException(ERROR_IN_TOPOLOGY + ": " + description + " not defined");
        }
    }

    protected interface TypeCompatibilityChecker {
        boolean compare(DataType type);
    }

    protected record TypeComparator(UserType type, TypeCompatibilityChecker checker, String faultDescription) {
    }

    protected TopologyException topologyError(String message) {
        return new TopologyException(ERROR_IN_TOPOLOGY + ": " + message);
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
    protected UserType firstSpecificType(FunctionDefinition func1, FunctionDefinition func2, FunctionDefinition func3) {
        return firstSpecificType(func1, func2, func3, UserType.UNKNOWN);
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of stream data types.
    protected UserType firstSpecificType(FunctionDefinition func1, FunctionDefinition func2, FunctionDefinition func3, StreamDataType... streamDataTypes) {
        return firstSpecificType(func1, func2, func3, toUserTypes(streamDataTypes));
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, UNKNOWN is returned.
    protected UserType firstSpecificType(FunctionDefinition func1, FunctionDefinition func2) {
        return firstSpecificType(func1, func2, UserType.UNKNOWN);
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of user types.
    protected UserType firstSpecificType(FunctionDefinition func1, FunctionDefinition func2, FunctionDefinition func3, UserType... userTypes) {
        return firstSpecificType(func1, func2, arrayFrom(func3.resultType(), userTypes));
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of stream data types.
    protected UserType firstSpecificType(FunctionDefinition func1, FunctionDefinition func2, StreamDataType... streamDataTypes) {
        return firstSpecificType(func1, func2, toUserTypes(streamDataTypes));
    }

    protected UserType firstSpecificType(FunctionDefinition func1, FunctionDefinition func2, UserType... userTypes) {
        return firstSpecificType(func1, arrayFrom(func2.resultType(), userTypes));
    }

    // Returns the first specific function result type in the sequence of functions, starting from the left. When
    // reaching the end of the list of functions, sequence is continued with the list of stream data types.
    protected UserType firstSpecificType(FunctionDefinition function, StreamDataType... streamDataTypes) {
        return firstSpecificType(function, toUserTypes(streamDataTypes));
    }

    protected UserType firstSpecificType(FunctionDefinition function, UserType... userTypes) {
        return firstSpecificType(arrayFrom(function.resultType(), userTypes));
    }

    // Returns the most specific type in the sequence by traversing the array and checking for DataType.UNKNOWNs. The
    // result is the first non-UNKNOWN, or otherwise the last entry in the array.
    protected UserType firstSpecificType(UserType... types) {
        for (int index = 0; index < types.length - 1; index++) {
            if (types[index].dataType() != DataType.UNKNOWN) return types[index];
        }
        return types[types.length - 1];
    }

    protected TypeComparator equalTo(StreamDataType compareType) {
        return equalTo(compareType.userType());
    }

    protected TypeComparator equalTo(UserType compareType) {
        return new TypeComparator(
                compareType,
                myDataType -> compareType.dataType().isAssignableFrom(myDataType) && myDataType.isAssignableFrom(compareType.dataType()),
                "of type " + compareType);
    }

    protected TypeComparator equalTo(DataType compareType) {
        return equalTo(new UserType(compareType));
    }

    protected TypeComparator superOf(StreamDataType compareType) {
        return superOf(compareType.userType());
    }

    protected TypeComparator superOf(UserType compareType) {
        return new TypeComparator(
                compareType,
                myDataType -> myDataType.isAssignableFrom(compareType.dataType()),
                "(superclass of) type " + compareType);
    }

    protected TypeComparator subOf(StreamDataType compareType) {
        return subOf(compareType.userType());
    }

    protected TypeComparator subOf(UserType compareType) {
        return new TypeComparator(
                compareType,
                myDataType -> compareType.dataType().isAssignableFrom(myDataType),
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
            throw topologyError(subject + " is expected to be " + comparator.faultDescription + ", but found " + type.schemaName());
        }
    }

    protected UserFunction userFunctionOf(TopologyBuildContext context, String functionType, FunctionDefinition function, StreamDataType resultType, TypeComparator... parameters) {
        return userFunctionOf(context, functionType, function, superOf(resultType), parameters);
    }

    protected UserFunction userFunctionOf(TopologyBuildContext context, String functionType, FunctionDefinition function, UserType resultType, TypeComparator... parameters) {
        return userFunctionOf(context, functionType, function, superOf(resultType), parameters);
    }

    protected UserFunction userFunctionOf(TopologyBuildContext context, String functionType, FunctionDefinition function, TypeComparator resultType, TypeComparator... parameters) {
        // Check if the function is defined
        if (function == null) return null;

        // Check if the resultType of the function is as expected
        checkType(functionType + " resultType", (function.resultType() != null ? function.resultType() : new UserType(UserType.DEFAULT_NOTATION, DataNull.DATATYPE)), resultType);
        // Update the applied result type of the function with the (more specific) supplied result type
        function = function.resultType() != null
                ? function.withResult(resultType.type())
                : function;

        // Check if the number of parameters is as expected
        int fixedParamCount = Arrays.stream(function.parameters()).map(p -> p.isOptional() ? 0 : 1).reduce(Integer::sum).orElse(0);
        if (fixedParamCount > parameters.length) {
            throw topologyError(functionType + " is expected to take at least " + fixedParamCount + " parameters");
        }
        if (function.parameters().length < parameters.length) {
            throw topologyError(functionType + " is expected to take at most " + function.parameters().length + " parameters");
        }

        // Check if all parameters are of expected type
        for (int index = 0; index < parameters.length; index++) {
            checkType(functionType + " parameter " + (index + 1) + " (\"" + function.parameters()[index].name() + "\")", function.parameters()[index].type(), superOf(parameters[index].type()));
        }

        // Here we replace the parameter types of the function with the (more specific) given types from the stream.
        // This allows user function wrappers to check incoming data types more strictly against expected types.
        final var newParams = new ParameterDefinition[function.parameters().length];
        // Replace the fixed parameters in the array
        for (int index = 0; index < parameters.length; index++) {
            final var param = function.parameters()[index];
            newParams[index] = new ParameterDefinition(param.name(), parameters[index].type().dataType(), param.isOptional(), param.defaultValue());
        }
        // Copy the remainder of the parameters into the new array
        System.arraycopy(function.parameters(), parameters.length, newParams, parameters.length, function.parameters().length - parameters.length);
        // Update the function with its new parameter types
        return context.createUserFunction(function.withParameters(newParams));
    }

    protected void checkTuple(String faultDescription, UserType type, DataType... elements) {
        checkTuple(faultDescription, type.dataType(), elements);
    }

    protected void checkTuple(String faultDescription, DataType type, DataType... elements) {
        if (!(type instanceof TupleType tupleType)) {
            throw new TopologyException(ERROR_IN_TOPOLOGY + ": " + faultDescription + " is expected to be a tuple");
        }
        if (tupleType.subTypeCount() != elements.length) {
            throw new TopologyException(ERROR_IN_TOPOLOGY + ": " + faultDescription + " is expected to be a tuple with " + elements.length + " elements");
        }
        for (int index = 0; index < elements.length; index++) {
            if (!elements[index].isAssignableFrom(tupleType.subType(index))) {
                throw new TopologyException(ERROR_IN_TOPOLOGY + ": " + faultDescription + " tuple element " + index + " is expected to be (subclass) of type " + elements[index]);
            }
        }
    }

    protected String[] combineStoreNames(String[]... storeNameArrays) {
        final var storeNames = new TreeSet<String>();
        for (String[] storeNameArray : storeNameArrays) {
            if (storeNameArray != null) Collections.addAll(storeNames, storeNameArray);
        }
        return storeNames.toArray(TEMPLATE);
    }

    protected StreamDataType streamDataTypeOf(DataType dataType, boolean isKey) {
        return streamDataTypeOf(new UserType(dataType), isKey);
    }

    protected StreamDataType streamDataTypeOf(String notationName, DataType dataType, boolean isKey) {
        return streamDataTypeOf(new UserType(notationName, dataType), isKey);
    }

    protected StreamDataType streamDataTypeOf(UserType userType, boolean isKey) {
        return new StreamDataType(userType, isKey);
    }

    protected StreamDataType windowedTypeOf(StreamDataType keyType) {
        return streamDataTypeOf(windowedTypeOf(keyType.userType()), true);
    }

    protected UserType windowedTypeOf(UserType keyType) {
        var windowedType = new WindowedType(keyType.dataType());
        return new UserType(keyType.notation(), windowedType);
    }

    protected Named namedOf() {
        return name != null ? Named.as(name) : null;
    }

    protected Grouped<Object, Object> groupedOf(StreamDataType k, StreamDataType v, KeyValueStateStoreDefinition store) {
        var grouped = Grouped.with(k.serde(), v.serde());
        if (name != null) grouped = grouped.withName(name);
        if (store != null) grouped = grouped.withName(store.name());
        return grouped;
    }

    protected Produced<Object, Object> producedOf(StreamDataType k, StreamDataType v, StreamPartitioner<Object, Object> partitioner) {
        var produced = Produced.with(k.serde(), v.serde());
        if (partitioner != null) produced = produced.withStreamPartitioner(partitioner);
        if (name != null) produced = produced.withName(name);
        return produced;
    }

    protected <V> Materialized<Object, V, KeyValueStore<Bytes, byte[]>> materializedOf(TopologyBuildContext context, KeyValueStateStoreDefinition store) {
        if (store != null) {
            return context.materialize(store);
        }
        return null;
    }

    protected <V> Materialized<Object, V, SessionStore<Bytes, byte[]>> materializedOf(TopologyBuildContext context, SessionStateStoreDefinition store) {
        if (store != null) {
            return context.materialize(store);
        }
        return null;
    }

    protected <V> Materialized<Object, V, WindowStore<Bytes, byte[]>> materializedOf(TopologyBuildContext context, WindowStateStoreDefinition store) {
        if (store != null) {
            return context.materialize(store);
        }
        return null;
    }

    protected Repartitioned<Object, Object> repartitionedOf(StreamDataType k, StreamDataType v, StreamPartitioner<Object, Object> partitioner) {
        if (partitioner == null) return null;
        var repartitioned = Repartitioned.with(k.serde(), v.serde()).withStreamPartitioner(partitioner);
        if (name != null) repartitioned = repartitioned.withName(name);
        return repartitioned;
    }

    protected Printed<Object, Object> printedOf(String filename, String label, KeyValueMapper<Object, Object, String> mapper) {
        var printed = filename != null ? Printed.toFile(filename) : Printed.toSysOut();
        if (label != null) printed = printed.withLabel(label);
        if (mapper != null) printed = printed.withKeyValueMapper(mapper);
        if (name != null) printed = printed.withName(name);
        return printed;
    }

    private record WrapPartitioner(
            StreamPartitioner<Object, Object> partitioner) implements StreamPartitioner<Object, Void> {
        @Override
        public Integer partition(String topic, Object key, Void value, int numPartitions) {
            return partitioner.partition(topic, key, value, numPartitions);
        }
    }

    protected TableJoined<Object, Object> tableJoinedOf(StreamPartitioner<Object, Object> partitioner, StreamPartitioner<Object, Object> otherPartitioner) {
        final var part = partitioner != null ? new WrapPartitioner(partitioner) : null;
        final var otherPart = otherPartitioner != null ? new WrapPartitioner(otherPartitioner) : null;
        return TableJoined.with(part, otherPart).withName(name);
    }
}
