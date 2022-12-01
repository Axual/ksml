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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.util.DataUtil;

public class UserKeyValueTransformer extends Invoker implements KeyValueMapper<Object, Object, KeyValue<Object, Object>> {
    public UserKeyValueTransformer(UserFunction function) {
        super(function);
        verifyParameterCount(2);
        verifyResultReturned(new TupleType(DataType.UNKNOWN, DataType.UNKNOWN));
    }

    @Override
    public KeyValue<Object, Object> apply(Object key, Object value) {
        var resultKeyType = ((TupleType) function.resultType.dataType()).subType(0);
        var resultValueType = ((TupleType) function.resultType.dataType()).subType(1);
        var result = function.call(DataUtil.asDataObject(key), DataUtil.asDataObject(value));
        return (KeyValue) function.convertToKeyValue(result, resultKeyType, resultValueType);
    }
}
