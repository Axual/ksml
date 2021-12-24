package io.axual.ksml.python;

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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.axual.ksml.exception.KSMLApplyException;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.user.UserFunction;

/**
 * Base class for stream operations.
 * Subclasses can implement Kafka Streams operations by subclassing this class and invoking
 * the {@link UserFunction} contained in it.
 */
public abstract class Invoker {
    private static final Logger LOG = LoggerFactory.getLogger(Invoker.class);
    protected final UserFunction function;

    protected Invoker(UserFunction function) {
        if (function == null) {
            throw new KSMLApplyException("Invoker: function can not be null");
        }
        this.function = function;
    }

    protected void verify(boolean condition, String errorMessage) {
        if (!condition) {
            throw new KSMLApplyException("This function can not be used as a " + getClass().getSimpleName() + ": " + errorMessage);
        }
    }

    protected void verifyParameterCount(int count) {
        verify(function.parameters.length == count, "Function needs " + count + " parameters");
    }

    protected void verifyNoResultReturned() {
        if (function.resultType != null) {
            LOG.warn("Function {} used as {}: Function return value will be ignored", function.name, getClass().getSimpleName());
        }
    }

    protected void verifyResultReturned(DataType expected) {
        verify(function.resultType != null, "Function does not return a result");
        verify(expected.isAssignableFrom(function.resultType.type()), "Function returns incompatible type: " + function.resultType.type() + ", expected " + expected);
    }

    protected void verifyResultType(DataType type) {
        verify(type.isAssignableFrom(function.resultType.type()), "Function returns " + function.resultType.type() + " instead of " + type);
    }
}
