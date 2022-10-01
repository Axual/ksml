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


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.ArrayList;
import java.util.List;

import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.KeyValueType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.exception.KSMLExecutionException;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.util.DataUtil;

public class UserKeyValueToKeyValueListTransformer extends Invoker implements KeyValueMapper<Object, Object, Iterable<KeyValue<Object, Object>>> {
    public UserKeyValueToKeyValueListTransformer(UserFunction function) {
        super(function);
        verifyParameterCount(2);
        verifyResultReturned(new ListType(new KeyValueType(DataType.UNKNOWN, DataType.UNKNOWN)));
    }

    @Override
    public Iterable<KeyValue<Object, Object>> apply(Object key, Object value) {
        var result = function.call(DataUtil.asUserObject(key), DataUtil.asUserObject(value));
        if (function.resultType.dataType() instanceof ListType listType &&
                listType.valueType() instanceof TupleType tupleType &&
                tupleType.subTypeCount() == 2) {
            var keyType = tupleType.subType(0);
            var valueType = tupleType.subType(1);

            if (result instanceof DataList) {
                var list = (List<DataObject>) result;
                var convertedResult = new ArrayList<KeyValue<Object, Object>>();
                for (DataObject element : list) {
                    KeyValue<DataObject, DataObject> convertedKeyValue = function.convertToKeyValue(element, keyType, valueType);
                    convertedResult.add((KeyValue) convertedKeyValue);
                }
                return convertedResult;
            }
        }

        throw new KSMLExecutionException("Expected list back from function: " + function.name);
    }
}
