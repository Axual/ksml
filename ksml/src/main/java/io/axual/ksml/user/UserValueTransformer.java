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
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

public class UserValueTransformer extends Invoker implements ValueMapperWithKey<Object, Object, DataObject> {
    private static final DataType EXPECTED_RESULT_TYPE = DataType.UNKNOWN;
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();

    public UserValueTransformer(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_VALUETRANSFORMER);
        verifyParameterCount(2);
    }

    @Override
    public DataObject apply(Object key, Object value) {
        verifyNoStoresUsed();
        return apply(null, key, value);
    }

    public DataObject apply(StateStores stores, Object key, Object value) {
        return timeExecutionOf(() -> function.call(stores, NATIVE_MAPPER.toDataObject(key), NATIVE_MAPPER.toDataObject(value)));
    }
}
