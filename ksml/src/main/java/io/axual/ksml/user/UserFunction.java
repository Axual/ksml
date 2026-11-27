package io.axual.ksml.user;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.InjectedParameter;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.store.StateStores;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;

import java.util.Arrays;
import java.util.Map;

@Slf4j
public class UserFunction {
    /** Injects the state stores of the call; the function's code reads each store by its name. */
    public static final String INJECT_STORES = "stores";
    @Getter
    protected final FunctionDefinition definition;

    public UserFunction(FunctionDefinition definition) {
        if (definition.namespace() == null)
            throw new TopologyException("Function namespace can not be null");
        if (definition.name() == null)
            throw new TopologyException("Function name can not be null");
        // A function that uses state stores gets them injected as stores
        this.definition = definition.storeNames().isEmpty() ? definition : definition.withInjectedParameter(new InjectedParameter(INJECT_STORES));
        // Call to check for proper order of fixed and optional parameters
        this.definition.fixedParameterCount();
        log.info("Registered function '{}'", this);
    }

    /** The injections for a call with these state stores; empty when stores is null or the function uses none. */
    public Map<String, Object> withStores(StateStores stores) {
        return stores != null && hasInjectedParameter(INJECT_STORES) ? Map.of(INJECT_STORES, stores) : Map.of();
    }

    /** Whether the function declares an injected parameter with this name. */
    public boolean hasInjectedParameter(String name) {
        return definition.injectedParameters().stream().anyMatch(param -> param.name().equals(name));
    }

    @Override
    public String toString() {
        String[] params = Arrays.stream(definition.parameters()).map(p -> p.name() + ":" + (p.type() != null ? p.type() : "?")).toArray(String[]::new);
        final var plural = definition.storeNames().size() > 1 ? "s" : "";
        final var storeSuffix = definition.storeNames().isEmpty()
                ? ""
                : " using store" + plural + " " + String.join(",", definition.storeNames());
        return definition.namespace() + "." + definition.name()
                + "(" + String.join(", ", params) + ")"
                + (definition.resultType() != null ? " ==> " + definition.resultType() : "")
                + storeSuffix;
    }

    protected void checkType(DataType expected, DataObject value) {
        if (value instanceof DataNull) return;
        if (expected != null && value != null) {
            final var assignable = expected.isAssignableFrom(value.type());
            if (assignable.isNotAssignable()) {
                throw DataException.conversionFailed(expected, value.type(), assignable);
            }
        }
    }

    protected void checkType(ParameterDefinition definition, DataObject value) {
        checkType(definition.type(), value);
    }

    protected void logCall(DataObject[] parameters, Object result) {
        // Log the called function in debug
        if (log.isDebugEnabled()) {
            // Check all parameters and copy them into the interpreter as prefixed globals
            StringBuilder paramsAndValues = new StringBuilder();
            for (int index = 0; index < parameters.length; index++) {
                paramsAndValues.append(paramsAndValues.isEmpty() ? "" : ", ");
                paramsAndValues.append(definition.parameters()[index].name()).append("=").append(parameters[index] != null ? parameters[index].toString(DataObject.Printer.EXTERNAL_ALL_SCHEMA) : "null");
            }
            if (result != null) {
                log.debug("User function {}.{}({}) returned {}", definition.namespace(), definition.name(), paramsAndValues, result);
            } else {
                log.debug("User function {}.{}({}) called", definition.namespace(), definition.name(), paramsAndValues);
            }
        }
    }

    /**
     * Call the user-defined function and return the result.
     *
     * @param injections variables to make available to the function's code, by name, such as {@link #INJECT_STORES}
     *                   and {@link io.axual.ksml.proxy.context.ProcessorContextProxy#INJECT_CONTEXT}; a name that is not given gets its default value.
     * @param parameters parameters for the function.
     * @return the result of the call.
     */
    public DataObject call(Map<String, Object> injections, DataObject... parameters) {
        throw new ExecutionException("Can not call the call() method of the UserFunction %s.%s directly. Override this class and the call() method.".formatted(definition.namespace(), definition.name()));
    }

    public final DataObject call(DataObject... parameters) {
        return call(Map.of(), parameters);
    }

    /** Returns the (key, value) result as a KeyValue. A None key or value becomes null, as Kafka Streams needs. */
    public KeyValue<Object, Object> convertToKeyValue(DataObject result, DataType keyType, DataType valueType) {
        if (result instanceof DataList list &&
                list.size() == 2 &&
                keyType.isAssignableFrom(list.get(0).type()).isAssignable() &&
                valueType.isAssignableFrom(list.get(1).type()).isAssignable()) {
            return new KeyValue<>(DataObject.nullIfNone(list.get(0)), DataObject.nullIfNone(list.get(1)));
        }

        if (result instanceof DataTuple tuple &&
                tuple.elements().size() == 2 &&
                keyType.isAssignableFrom(tuple.elements().get(0).type()).isAssignable() &&
                valueType.isAssignableFrom(tuple.elements().get(1).type()).isAssignable()) {
            return new KeyValue<>(DataObject.nullIfNone(tuple.elements().get(0)), DataObject.nullIfNone(tuple.elements().get(1)));
        }

        throw new TopologyException("Function %s.%s - Expected (key, value) from function {} but got: %s".formatted(definition.namespace(), definition.name(), (result != null ? result : "null")));
    }
}
