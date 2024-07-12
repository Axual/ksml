package io.axual.ksml.python;

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


import com.codahale.metrics.Timer;
import io.axual.ksml.data.notation.UserType;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.metric.MetricName;
import io.axual.ksml.metric.Metrics;
import io.axual.ksml.user.UserFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public abstract class Invoker {
    private static final Logger LOG = LoggerFactory.getLogger(Invoker.class);
    protected final UserFunction function;
    private final Timer timer;

    protected Invoker(UserFunction function, ContextTags tags, String functionType) {
        if (function == null) {
            throw new TopologyException("Invoker: function can not be null");
        }
        this.function = function;

        final var metricTags = tags.append("function-type", functionType).append("function-name", this.function.name);
        final var metricName = new MetricName("execution-time", metricTags);
        if (Metrics.registry().getTimer(metricName) == null) {
            timer = Metrics.registry().registerTimer(metricName);
        } else {
            timer = Metrics.registry().getTimer(metricName);
        }
    }

    protected <V> V timeExecutionOf(Supplier<V> callback) {
        return timer.timeSupplier(callback);
    }

    protected void verify(boolean condition, String errorMessage) {
        if (!condition) {
            throw new TopologyException("This function can not be used as a " + getClass().getSimpleName() + ": " + errorMessage);
        }
    }

    protected void verifyParameterCount(int count) {
        verify(function.fixedParameterCount <= count, "Function needs at least " + count + " parameters");
        verify(function.parameters.length >= count, "Function needs at most " + count + " parameters");
    }

    protected void verifyNoResult() {
        verifyNoResultInternal(function.resultType);
    }

    private void verifyNoResultInternal(UserType type) {
        if (type != null && type.dataType() != DataNull.DATATYPE) {
            LOG.warn("Function {} used as {}: Function return value of type " + type + " will be ignored", function.name, getClass().getSimpleName());
        }
    }

    protected void verifyResultType(DataType expected) {
        verifyTypeInternal(function.resultType, expected);
    }

    private void verifyTypeInternal(UserType type, DataType expected) {
        verify(type != null, "Function does not return a result, while " + expected + " was expected");
        verify(expected.isAssignableFrom(type.dataType()), "Function does not return expected " + expected + ". but " + type.dataType() + " instead");
    }

    protected void verifyNoStoresUsed() {
        verify(function.storeNames.length == 0, getClass().getSimpleName() + " function uses state stores in a context where it can not: " + String.join(",", function.storeNames));
    }
}
