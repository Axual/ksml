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
import io.axual.ksml.operation.processor.OperationProcessorSupplier;
import io.axual.ksml.operation.processor.PeekProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserForeachAction;
import io.axual.ksml.user.UserFunction;
import org.apache.kafka.streams.kstream.Named;

public class PeekOperation extends BaseOperation {
    private static final String FOREACHACTION_NAME = "ForEachAction";
    private final UserFunction forEachAction;

    public PeekOperation(OperationConfig config, UserFunction forEachAction) {
        super(config);
        this.forEachAction = forEachAction;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    KTable<K, V> filterNot(
         *          final Predicate<? super K, ? super V> predicate
         *          final Named named)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        checkFunction(FOREACHACTION_NAME, forEachAction, equalTo(DataNull.DATATYPE), superOf(k), superOf(v));

        final var action = new UserForeachAction(forEachAction);
        final var storeNames = combineStoreNames(this.storeNames, forEachAction.storeNames);
        final var supplier = new OperationProcessorSupplier<>(
                name,
                PeekProcessor::new,
                (stores, record) -> action.apply(stores, record.key(), record.value()),
                storeNames);
        final var output = name != null
                ? input.stream.process(supplier, Named.as(name), storeNames)
                : input.stream.process(supplier, storeNames);
        return new KStreamWrapper(output, k, v);
    }
}
