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


import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.processor.FixedKeyOperationProcessorSupplier;
import io.axual.ksml.operation.processor.PeekProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserForeachAction;
import org.apache.kafka.streams.kstream.Named;

public class PeekOperation extends BaseOperation {
    private static final String FOREACHACTION_NAME = "ForEachAction";
    private final FunctionDefinition forEachAction;

    public PeekOperation(OperationConfig config, FunctionDefinition forEachAction) {
        super(config);
        this.forEachAction = forEachAction;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        final var k = input.keyType();
        final var v = input.valueType();
        final var action = userFunctionOf(context, FOREACHACTION_NAME, forEachAction, equalTo(DataNull.DATATYPE), superOf(k.flatten()), superOf(v.flatten()));
        final var userAction = new UserForeachAction(action, tags);
        final var storeNames = forEachAction.storeNames().toArray(String[]::new);
        final var supplier = new FixedKeyOperationProcessorSupplier<>(
                name,
                PeekProcessor::new,
                (stores, rec) -> userAction.apply(stores, flattenValue(rec.key()), flattenValue(rec.value())),
                storeNames);
        final var output = name != null
                ? input.stream.processValues(supplier, Named.as(name), storeNames)
                : input.stream.processValues(supplier, storeNames);
        return new KStreamWrapper(output, k, v);
    }
}
