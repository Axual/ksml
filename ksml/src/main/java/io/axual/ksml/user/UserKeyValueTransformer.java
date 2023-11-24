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


import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.store.StateStores;
import io.axual.ksml.util.DataUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class UserKeyValueTransformer extends Invoker implements KeyValueMapper<Object, Object, KeyValue<Object, Object>> {
    private final static DataType EXPECTED_RESULT_TYPE = new TupleType(DataType.UNKNOWN, DataType.UNKNOWN);

    public UserKeyValueTransformer(UserFunction function) {
        super(function);
        verifyParameterCount(2);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public KeyValue<Object, Object> apply(Object key, Object value) {
        verifyNoStoresUsed();
        return apply(null, key, value);
    }

    public KeyValue<Object, Object> apply(StateStores stores, Object key, Object value) {
        verifyAppliedResultType(EXPECTED_RESULT_TYPE);
        final var kr = ((TupleType) function.appliedResultType.dataType()).subType(0);
        final var vr = ((TupleType) function.appliedResultType.dataType()).subType(1);
        final var result = function.call(stores, DataUtil.asDataObject(key), DataUtil.asDataObject(value));
        return function.convertToKeyValue(result, kr, vr);
    }
}
