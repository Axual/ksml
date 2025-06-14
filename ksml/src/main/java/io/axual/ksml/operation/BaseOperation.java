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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.data.type.WindowedType;
import io.axual.ksml.definition.*;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserValueJoiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

@Slf4j
public abstract class BaseOperation implements StreamOperation {
    private static final String ERROR_IN_TOPOLOGY = "Error in topology";

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

    private static final DataObjectFlattener DATA_OBJECT_FLATTENER = new DataObjectFlattener();
    protected final String name;
    protected final MetricTags tags;

    protected BaseOperation(OperationConfig config) {
        var error = NameValidator.validateNameAndReturnError(config.name());
        if (error != null) {
            log.warn("Ignoring name with error '" + config.name() + "': " + error);
            name = null;
        } else {
            name = config.name();
        }
        tags = config.tags().append("operation-name", name);
    }

    @Override
    public String toString() {
        var operation = getClass().getSimpleName();
        if (operation.toLowerCase().endsWith("operation")) {
            operation = operation.substring(0, operation.length() - 9);
        }
        return (name == null ? "Unnamed" : name) + " operation " + operation;
    }

    protected DataObject flattenValue(Object value) {
        return DATA_OBJECT_FLATTENER.toDataObject(value);
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
            throw topologyError(subject + " is expected to be " + comparator.faultDescription + ", but found " + type.name());
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

    protected StreamDataType streamDataTypeOf(DataType dataType, boolean isKey) {
        return streamDataTypeOf(new UserType(dataType), isKey);
    }

    protected StreamDataType streamDataTypeOf(String notationName, DataType dataType, boolean isKey) {
        return streamDataTypeOf(new UserType(notationName, dataType), isKey);
    }

    protected StreamDataType streamDataTypeOf(UserType userType, boolean isKey) {
        return new StreamDataType(userType, isKey);
    }

    protected StreamDataType windowed(StreamDataType keyType) {
        return streamDataTypeOf(windowed(keyType.userType()), true);
    }

    protected UserType windowed(UserType keyType) {
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

    protected Repartitioned<Object, Object> repartitionedOf(StreamDataType k, StreamDataType v, Integer numberOfPartitions, StreamPartitioner<Object, Object> partitioner) {
        if (partitioner == null) return null;
        var repartitioned = Repartitioned.with(k.serde(), v.serde()).withStreamPartitioner(partitioner);
        if (numberOfPartitions != null) repartitioned = repartitioned.withNumberOfPartitions(numberOfPartitions);
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

    protected Joined<Object, Object, Object> joinedOf(StreamDataType k, StreamDataType v, StreamDataType vo, Duration gracePeriod) {
        var result = Joined.with(k.serde(), v.serde(), vo.serde());
        if (name != null) result = result.withName(name);
        if (gracePeriod != null) result = result.withGracePeriod(gracePeriod);
        return result;
    }

    protected StreamJoined<Object, Object, Object> streamJoinedOf(WindowStateStoreDefinition thisStore, WindowStateStoreDefinition otherStore, StreamDataType k, StreamDataType v, StreamDataType vo, JoinWindows joinWindows) {
        var result = StreamJoined.with(k.serde(), v.serde(), vo.serde()).withLoggingDisabled();
        if (name != null) result = result.withName(name);
        if (thisStore != null) {
            if (thisStore.name() != null) result = result.withStoreName(thisStore.name());
            if (thisStore.logging()) result = result.withLoggingEnabled(new HashMap<>());
            result = result.withThisStoreSupplier(validateWindowStore(thisStore, joinWindows));
        }
        if (otherStore != null) {
            result = result.withOtherStoreSupplier(validateWindowStore(otherStore, joinWindows));
        }
        return result;
    }

    private WindowBytesStoreSupplier validateWindowStore(WindowStateStoreDefinition store, JoinWindows joinWindows) {
        // Copied these validation rules from Kafka Streams' KStreamImplJoin::assertWindowSettings.
        // The checks are duplicated here, since the Kafka Streams error message for invalid window store configs is
        // really cryptic and makes no sense to an average user.
        final var result = StoreUtil.getStoreSupplier(store);
        if (!result.retainDuplicates()) {
            throw new TopologyException("The window store '" + store.name() + "' should have 'retainDuplicates' set to 'true'.");
        }
        if (result.windowSize() != joinWindows.size()) {
            throw new TopologyException("The window store '" + store.name() + "' should have 'windowSize' equal to '2*timeDifference'.");
        }
        if (result.retentionPeriod() != joinWindows.size() + joinWindows.gracePeriodMs()) {
            throw new TopologyException("The window store '" + store.name() + "' should have 'retention' equal to '2*timeDifference + grace'.");
        }
        return result;
    }

    private record WrapPartitioner(
            StreamPartitioner<Object, Object> partitioner) implements StreamPartitioner<Object, Void> {
        @Override
        public Optional<Set<Integer>> partitions(String topic, Object key, Void value, int numPartitions) {
            return partitioner.partitions(topic, key, value, numPartitions);
        }
    }

    protected TableJoined<Object, Object> tableJoinedOf(StreamPartitioner<Object, Object> partitioner, StreamPartitioner<Object, Object> otherPartitioner) {
        final var part = partitioner != null ? new WrapPartitioner(partitioner) : null;
        final var otherPart = otherPartitioner != null ? new WrapPartitioner(otherPartitioner) : null;
        return TableJoined.with(part, otherPart).withName(name);
    }

    protected JoinWindows joinWindowsOf(Duration timeDifference, Duration gracePeriod) {
        if (gracePeriod != null) {
            return JoinWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod);
        }
        return JoinWindows.ofTimeDifferenceWithNoGrace(timeDifference);
    }

    protected ValueJoiner<Object, Object, Object> valueJoiner(UserFunction function, MetricTags tags) {
        return new UserValueJoiner(function, tags);
    }

    protected ValueJoinerWithKey<Object, Object, Object, Object> valueJoinerWithKey(UserFunction function, MetricTags tags) {
        return new UserValueJoiner(function, tags);
    }

    protected KeyValueStateStoreDefinition validateKeyValueStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateKeyValueStore(store, keyType.userType(), valueType.userType());
    }

    protected KeyValueStateStoreDefinition validateKeyValueStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof KeyValueStateStoreDefinition keyValueStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : keyValueStore.keyType();
            final var storeValueType = valueType != null ? valueType : keyValueStore.valueType();
            return new KeyValueStateStoreDefinition(
                    keyValueStore.name(),
                    keyValueStore.persistent(),
                    keyValueStore.timestamped(),
                    keyValueStore.versioned(),
                    keyValueStore.historyRetention(),
                    keyValueStore.segmentInterval(),
                    storeKeyType,
                    storeValueType,
                    keyValueStore.caching(),
                    keyValueStore.logging());
        }
        throw new ExecutionException(this + " requires a  state store of type 'keyValue'");
    }

    protected SessionStateStoreDefinition validateSessionStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateSessionStore(store, keyType.userType(), valueType.userType());
    }

    protected SessionStateStoreDefinition validateSessionStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof SessionStateStoreDefinition sessionStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : sessionStore.keyType();
            final var storeValueType = valueType != null ? valueType : sessionStore.valueType();
            return new SessionStateStoreDefinition(
                    sessionStore.name(),
                    sessionStore.persistent(),
                    sessionStore.timestamped(),
                    sessionStore.retention(),
                    storeKeyType,
                    storeValueType,
                    sessionStore.caching(),
                    sessionStore.logging());
        }
        throw new ExecutionException(this + " requires a  state store of type 'session'");
    }

    protected WindowStateStoreDefinition validateWindowStore(StateStoreDefinition store, StreamDataType keyType, StreamDataType valueType) {
        return validateWindowStore(store, keyType.userType(), valueType.userType());
    }

    protected WindowStateStoreDefinition validateWindowStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        if (store == null) return null;
        if (store instanceof WindowStateStoreDefinition windowStore) {
            validateStore(store, keyType, valueType);
            final var storeKeyType = keyType != null ? keyType : windowStore.keyType();
            final var storeValueType = valueType != null ? valueType : windowStore.valueType();
            return new WindowStateStoreDefinition(
                    windowStore.name(),
                    windowStore.persistent(),
                    windowStore.timestamped(),
                    windowStore.retention(),
                    windowStore.windowSize(),
                    windowStore.retainDuplicates(),
                    storeKeyType,
                    storeValueType,
                    windowStore.caching(),
                    windowStore.logging());
        }
        throw new ExecutionException(this + " requires a  state store of type 'window'");
    }

    private static void validateStore(StateStoreDefinition store, UserType keyType, UserType valueType) {
        validateStoreTypeWithStreamType(store, "key", store.keyType(), keyType);
        validateStoreTypeWithStreamType(store, "value", store.valueType(), valueType);
    }

    private static void validateStoreTypeWithStreamType(StateStoreDefinition store, String keyOrValue, UserType storeKeyOrValueType, UserType streamKeyOrValueType) {
        if (streamKeyOrValueType == null) {
            if (storeKeyOrValueType == null) {
                throw new ExecutionException("State store '" + store.name() + "' does not have a defined " + keyOrValue + " type");
            }
            return;
        }

        if (storeKeyOrValueType != null && !storeKeyOrValueType.dataType().isAssignableFrom(streamKeyOrValueType.dataType())) {
            throw new ExecutionException("Incompatible " + keyOrValue + " types for state store '" + store.name() + "': " + storeKeyOrValueType + " and " + streamKeyOrValueType);
        }
    }
}
