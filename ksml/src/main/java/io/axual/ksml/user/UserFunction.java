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

import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.store.StateStores;

public class UserFunction {
    private static final Logger LOG = LoggerFactory.getLogger(UserFunction.class);
    private static final String[] TEMPLATE = new String[]{};
    public final String name;
    public final String namespace;
    public final ParameterDefinition[] parameters;
    public final int fixedParameterCount;
    public final UserType resultType;
    public final String[] storeNames;

    public UserFunction(String namespace, String name, ParameterDefinition[] parameters, UserType resultType, List<String> storeNames) {
        this(namespace, name, parameters, resultType, storeNames != null ? storeNames.toArray(TEMPLATE) : TEMPLATE);
    }

    public UserFunction(String namespace, String name, ParameterDefinition[] parameters, UserType resultType, String[] storeNames) {
        if (namespace == null) {
            throw new TopologyException("Function namespace can not be null");
        }
        if (name == null) {
            throw new TopologyException("Function name can not be null");
        }
        this.namespace = namespace;
        this.name = name;
        this.parameters = parameters;
        this.fixedParameterCount = getFixedParameterCount(parameters);
        this.resultType = resultType;
        this.storeNames = storeNames != null ? storeNames : TEMPLATE;
        LOG.info("Registered function '{}'", this);
    }

    @Override
    public String toString() {
        String[] params = Arrays.stream(parameters).map(p -> p.name() + ":" + (p.type() != null ? p.type() : "?")).toArray(String[]::new);
        return namespace + "." + name
                + "(" + String.join(", ", params) + ")"
                + (resultType != null ? " ==> " + resultType : "")
                + (storeNames.length > 0 ? " using store" + (storeNames.length > 1 ? "s" : "") + " " + String.join(",", storeNames) : "");
    }

    // Count the number of fixed parameters. Throw an error if the ordering is illegal (ie. fixed parameters should
    // always come before optional parameters in the params list)
    private int getFixedParameterCount(ParameterDefinition[] parameters) {
        var inOptionals = false;
        var fixedParamCount = 0;
        for (final var param : parameters) {
            if (!param.isOptional()) {
                if (inOptionals) {
                    throw new TopologyException("Error in parameter list of %s.%s, fixed parameters should be listed first: %s".formatted(namespace, name, Arrays.toString(parameters)));
                }
                fixedParamCount++;
            } else {
                inOptionals = true;
            }
        }
        return fixedParamCount;
    }

    protected void checkType(DataType expected, DataObject value) {
        if (value instanceof DataNull) return;
        if (expected != null && value != null && !expected.isAssignableFrom(value.type())) {
            throw DataException.conversionFailed(expected, value.type());
        }
    }

    protected void checkType(ParameterDefinition definition, DataObject value) {
        checkType(definition.type(), value);
    }

    protected void logCall(DataObject[] parameters, Object result) {
        // Log the called function in debug
        if (LOG.isDebugEnabled()) {
            // Check all parameters and copy them into the interpreter as prefixed globals
            StringBuilder paramsAndValues = new StringBuilder();
            for (int index = 0; index < parameters.length; index++) {
                paramsAndValues.append(!paramsAndValues.isEmpty() ? ", " : "");
                paramsAndValues.append(this.parameters[index].name()).append("=").append(parameters[index] != null ? parameters[index].toString(DataObject.Printer.EXTERNAL_ALL_SCHEMA) : "null");
            }
            if (result != null) {
                LOG.debug("User function {}.{}({}) returned {}", namespace, name, paramsAndValues, result);
            } else {
                LOG.debug("User function {}.{}({}) called", namespace, name, paramsAndValues);
            }
        }
    }

    /**
     * Call the user-defined function and return the result.
     *
     * @param parameters parameters for the function.
     * @return the result of the call.
     */
    public DataObject call(StateStores stores, DataObject... parameters) {
        throw new ExecutionException("Can not call the call() method of the UserFunction %s.%s directly. Override this class and the call() method.".formatted(namespace, name));
    }

    public final DataObject call(DataObject... parameters) {
        return call(null, parameters);
    }

    public KeyValue<Object, Object> convertToKeyValue(DataObject result, DataType keyType, DataType valueType) {
        if (result instanceof DataList list &&
                list.size() == 2 &&
                keyType.isAssignableFrom(list.get(0).type()) &&
                valueType.isAssignableFrom(list.get(1).type())) {
            return new KeyValue<>(list.get(0), list.get(1));
        }

        if (result instanceof DataTuple tuple &&
                tuple.size() == 2 &&
                keyType.isAssignableFrom(tuple.get(0).type()) &&
                valueType.isAssignableFrom(tuple.get(1).type())) {
            return new KeyValue<>(tuple.get(0), tuple.get(1));
        }

        throw new TopologyException("Function %s.%s - Expected (key, value) from function {} but got: %s".formatted(namespace, name, (result != null ? result : "null")));
    }
}
