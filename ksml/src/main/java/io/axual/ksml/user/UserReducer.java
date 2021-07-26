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


import org.apache.kafka.streams.kstream.Reducer;

import io.axual.ksml.util.DataUtil;
import io.axual.ksml.python.Invoker;

public class UserReducer extends Invoker implements Reducer<Object> {
    public UserReducer(UserFunction function) {
        super(function);
        verifyParameterCount(2);
        verify(function.parameters[0].type.equals(function.parameters[1].type), "Reducer should take two parameters of the same type");
        verify(function.parameters[0].type.equals(function.resultType), "Reducer should return same type as its parameters");
    }

    @Override
    public Object apply(Object value1, Object value2) {
        return function.call(DataUtil.asData(value1), DataUtil.asData(value2));
    }
}
