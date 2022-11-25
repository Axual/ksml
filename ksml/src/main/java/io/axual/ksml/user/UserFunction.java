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

import java.util.Arrays;

import io.axual.ksml.data.object.user.UserList;
import io.axual.ksml.data.object.user.UserObject;
import io.axual.ksml.data.object.user.UserTuple;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.exception.KSMLTypeException;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.util.StringUtil;

/**
 * Base class for user-defined functions.
 * Currently there is one subclass {@link io.axual.ksml.python.PythonFunction}, which handles Python based functions.
 */
public abstract class UserFunction {
    private static final Logger LOG = LoggerFactory.getLogger(UserFunction.class);
    public final String name;
    public final ParameterDefinition[] parameters;
    public final UserType resultType;

    public UserFunction(String name, ParameterDefinition[] parameters, UserType resultType) {
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

    protected void checkType(DataType expected, UserObject value) {
        if (expected != null && value != null && !expected.isAssignableFrom(value.type().type())) {
            throw KSMLTypeException.conversionFailed(expected, value.type().type());
        }
    }

    protected void checkType(ParameterDefinition definition, UserObject value) {
        checkType(definition.type, value);
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
    public abstract UserObject call(UserObject... parameters) ;
//    {
//        throw new KSMLExecutionException("Can not call the call() method of a UserFunction directly. Override this class and the call() method.");
//    }

    private KSMLTopologyException validateException(Object result, String expectedType) {
        return new KSMLTopologyException("Expected " + expectedType + " from function " + name + " but got: " + (result != null ? result : "null"));
    }

    public KeyValue<UserObject, UserObject> convertToKeyValue(UserObject result, UserType keyType, UserType valueType) {
        if (result instanceof UserList) {
            var list = (UserList) result;
            if (list.size() == 2 && keyType.isAssignableFrom(list.get(0).type()) && valueType.isAssignableFrom(list.get(1).type())) {
                return new KeyValue<>(list.get(0), list.get(1));
            }
        }

        if (result instanceof UserTuple) {
            var tuple = (UserTuple) result;
            if (tuple.size() == 2 && keyType.isAssignableFrom(tuple.get(0).type()) && valueType.isAssignableFrom(tuple.get(1).type())) {
                return new KeyValue<>(tuple.get(0), tuple.get(1));
            }
        }

        throw validateException(result, "(key,value)");
    }
}
