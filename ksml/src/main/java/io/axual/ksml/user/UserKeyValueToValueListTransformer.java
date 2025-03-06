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

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

import java.util.ArrayList;

public class UserKeyValueToValueListTransformer extends Invoker implements ValueMapperWithKey<Object, Object, Iterable<Object>> {
    private static final DataType EXPECTED_RESULT_TYPE = new ListType(DataType.UNKNOWN);
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();

    public UserKeyValueToValueListTransformer(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_KEYVALUETOVALUELISTTRANSFORMER);
        verifyParameterCount(2);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public Iterable<Object> apply(Object key, Object value) {
        verifyNoStoresUsed();
        return apply(null, key, value);
    }

    public Iterable<Object> apply(StateStores stores, Object key, Object value) {
        final var result = timeExecutionOf(() -> function.call(stores, NATIVE_MAPPER.toDataObject(key), NATIVE_MAPPER.toDataObject(value)));
        if (result instanceof DataList list) {
            final var newList = new ArrayList<>();
            list.forEach(newList::add);
            return newList;
        }
        throw new ExecutionException("Expected list result from keyValueToKeyValueList transformer: " + function.name);
    }
}
