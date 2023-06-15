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


import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.store.StateStores;
import io.axual.ksml.util.DataUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.ArrayList;
import java.util.Collections;

public class UserKeyValueToKeyValueListTransformer extends Invoker implements KeyValueMapper<Object, Object, Iterable<KeyValue<Object, Object>>> {
    protected final DataType resultKeyType;
    protected final DataType resultValueType;

    public UserKeyValueToKeyValueListTransformer(UserFunction function) {
        super(function);
        verifyParameterCount(2);
        verifyResultReturned(new ListType(new TupleType(DataType.UNKNOWN, DataType.UNKNOWN)));

        // If the above check worked, then we can safely perform the following cast
        var tupleType = (TupleType) ((ListType) function.resultType.dataType()).valueType();
        resultKeyType = tupleType.subType(0);
        resultValueType = tupleType.subType(1);
    }

    @Override
    public Iterable<KeyValue<Object, Object>> apply(Object key, Object value) {
        verifyNoStoresUsed();
        return apply(null, key, value);
    }

    public Iterable<KeyValue<Object, Object>> apply(StateStores stores, Object key, Object value) {
        var result = function.call(stores, DataUtil.asDataObject(key), DataUtil.asDataObject(value));
        if (result == null) return Collections.emptyList();

        // We need to convert the resulting messages to KeyValue tuples as per the method signature
        if (result instanceof DataList list) {
            var convertedResult = new ArrayList<KeyValue<Object, Object>>();
            for (DataObject element : list) {
                var convertedKeyValue = function.convertToKeyValue(element, resultKeyType, resultValueType);
                convertedResult.add(convertedKeyValue);
            }
            return convertedResult;
        }

        throw new KSMLExecutionException("Expected list back from function: " + function.name);
    }
}
