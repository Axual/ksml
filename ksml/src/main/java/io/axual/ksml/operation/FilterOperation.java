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


import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.operation.processor.FilterProcessor;
import io.axual.ksml.operation.processor.OperationProcessorSupplier;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserPredicate;
import org.apache.kafka.streams.kstream.Named;

public class FilterOperation extends BaseOperation {
    private static final String PREDICATE_NAME = "Predicate";
    private final UserFunction predicate;

    public FilterOperation(OperationConfig config, UserFunction predicate) {
        super(config);
        this.predicate = predicate;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        /*    Kafka Streams method signature:
         *    KStream<K, V> filter(
         *          final Predicate<? super K, ? super V> predicate
         *          final Named named)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        checkFunction(PREDICATE_NAME, predicate, new UserType(DataBoolean.DATATYPE), superOf(k), superOf(v));

        final var action = new UserPredicate(predicate);
        final var storeNames = combineStoreNames(this.storeNames, predicate.storeNames);
        final var supplier = new OperationProcessorSupplier<>(
                name,
                FilterProcessor::new,
                (stores, record) -> action.test(stores, record.key(), record.value()),
                storeNames);
        final var output = name != null
                ? input.stream.process(supplier, Named.as(name), storeNames)
                : input.stream.process(supplier, storeNames);
        return new KStreamWrapper(output, k, v);
    }

    @Override
    public StreamWrapper apply(KTableWrapper input) {
        /*    Kafka Streams method signature:
         *    KTable<K, V> filterNot(
         *          final Predicate<? super K, ? super V> predicate
         *          final Named named)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        checkFunction(PREDICATE_NAME, predicate, new UserType(DataBoolean.DATATYPE), superOf(k), superOf(v));
        final var output = name != null
                ? input.table.filter(new UserPredicate(predicate), Named.as(name))
                : input.table.filter(new UserPredicate(predicate));
        return new KTableWrapper(output, k, v);
    }
}
