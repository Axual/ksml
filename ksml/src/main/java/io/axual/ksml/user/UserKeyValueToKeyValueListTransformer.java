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
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ExecutionException;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.ArrayList;
import java.util.Collections;

public class UserKeyValueToKeyValueListTransformer extends Invoker implements KeyValueMapper<Object, Object, Iterable<KeyValue<Object, Object>>> {
    private static final DataType EXPECTED_RESULT_TYPE = new ListType(new TupleType(DataType.UNKNOWN, DataType.UNKNOWN));
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();

    public UserKeyValueToKeyValueListTransformer(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_KEYVALUETOKEYVALUELISTTRANSFORMER);
        verifyParameterCount(2);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public Iterable<KeyValue<Object, Object>> apply(Object key, Object value) {
        verifyNoStoresUsed();
        return apply(null, key, value);
    }

    public Iterable<KeyValue<Object, Object>> apply(StateStores stores, Object key, Object value) {
        // If the above check worked, then we can safely perform the following cast
        final var tupleType = (TupleType) ((ListType) function.resultType.dataType()).valueType();
        final var kr = tupleType.subType(0);
        final var vr = tupleType.subType(1);

        final var result = timeExecutionOf(() -> function.call(stores, NATIVE_MAPPER.toDataObject(key), NATIVE_MAPPER.toDataObject(value)));
        if (result == null) return Collections.emptyList();

        // We need to convert the resulting messages to KeyValue tuples as per the method signature
        if (result instanceof DataList list) {
            final var convertedResult = new ArrayList<KeyValue<Object, Object>>();
            for (DataObject element : list) {
                final var convertedKeyValue = function.convertToKeyValue(element, kr, vr);
                convertedResult.add(convertedKeyValue);
            }
            return convertedResult;
        }

        throw new ExecutionException("Expected list back from function: " + function.name);
    }
}
