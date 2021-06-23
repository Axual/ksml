package io.axual.ksml.user;

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


import org.apache.kafka.streams.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.exception.KSMLTypeException;
import io.axual.ksml.type.DataType;
import io.axual.ksml.type.Tuple;
import io.axual.ksml.util.StringUtil;

/**
 * Base class for user-defined functions.
 * Currently there is one subclass {@link io.axual.ksml.python.PythonFunction}, which handles Python based functions.
 */
public abstract class UserFunction {
    private static final Logger LOG = LoggerFactory.getLogger(UserFunction.class);
    public final String name;
    public final ParameterDefinition[] parameters;
    public final DataType resultType;

    public UserFunction(String name, ParameterDefinition[] parameters, DataType resultType) {
        this.name = name;
        this.parameters = parameters;
        this.resultType = resultType;
        LOG.info("Registered function '{}'", this);
    }

    @Override
    public String toString() {
        String[] params = Arrays.stream(parameters).map(p -> p.name + ":" + (p.type != null ? p.type : "?")).toArray(String[]::new);
        return name + "(" + StringUtil.join(", ", params) + ")" + (resultType != null ? " ==> " + resultType : "");
    }

    protected void checkType(Object value, DataType expected) {
        if (expected != null && value != null && !expected.isAssignableFrom(value.getClass())) {
            throw KSMLTypeException.conversionFailed(expected, value.getClass());
        }
    }

    protected void checkType(ParameterDefinition definition, Object value) {
        checkType(value, definition.type);
    }

    protected void logCall(Object[] parameters, Object result) {
        // Log the called function in debug
        if (LOG.isDebugEnabled()) {
            // Check all parameters and copy them into the interpreter as prefixed globals
            StringBuilder paramsAndValues = new StringBuilder();
            for (int index = 0; index < parameters.length; index++) {
                paramsAndValues.append(paramsAndValues.length() > 0 ? ", " : "");
                String valueQuote = parameters[index] instanceof String ? "'" : "";
                paramsAndValues.append(this.parameters[index].name).append("=").append(valueQuote).append(parameters[index] != null ? parameters[index] : "null").append(valueQuote);
            }
            if (result != null) {
                LOG.debug("User function {}({}) returned {}", name, paramsAndValues, result);
            } else {
                LOG.debug("User function {}({}) called", name, paramsAndValues);
            }
        }
    }

    /**
     * Call the user-defined function and return the result.
     *
     * @param parameters parameters for the function.
     * @return the result of the call.
     */
    public abstract Object call(Object... parameters);

    private KSMLTopologyException validateException(Object result, String expectedType) {
        return new KSMLTopologyException("Expected " + expectedType + " from function " + name + " but got: " + (result != null ? result : "null"));
    }

    public KeyValue<Object, Object> convertToKeyValue(Object result, DataType keyType, DataType valueType) {
        if (result instanceof KeyValue) {
            KeyValue<?, ?> kv = (KeyValue<?, ?>) result;
            if (keyType.isAssignableFrom(kv.key) && valueType.isAssignableFrom(kv.value)) {
                return new KeyValue<>(kv.key, kv.value);
            }
        }

        if (result instanceof Tuple) {
            Tuple kv = (Tuple) result;
            if (kv.size() == 2 && keyType.isAssignableFrom(kv.get(0)) && valueType.isAssignableFrom(kv.get(1))) {
                return new KeyValue<>(kv.get(0), kv.get(1));
            }
        }

        throw validateException(result, "(key,value)");
    }

    public Iterable<Object> convertToList(Object result, DataType valueType) {
        if (result instanceof List) {
            List<?> list = (List<?>) result;
            List<Object> convertedResult = new ArrayList<>();
            for (Object element : list) {
                checkType(element, valueType);
                convertedResult.add(element);
            }
            return convertedResult;
        }
        throw validateException(result, "[value1,value2,...]");
    }

    public Iterable<KeyValue<Object, Object>> convertToKeyValueList(Object result, DataType keyType, DataType valueType) {
        if (result instanceof List) {
            List<?> list = (List<?>) result;
            List<KeyValue<Object, Object>> convertedResult = new ArrayList<>();
            for (Object element : list) {
                convertedResult.add(convertToKeyValue(element, keyType, valueType));
            }
            return convertedResult;
        }
        throw validateException(result, "[(key1,value1),(key2,value2),...]");
    }
}
