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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.python.Invoker;
import org.apache.kafka.streams.kstream.Reducer;

public class UserReducer extends Invoker implements Reducer<Object> {
    private final NativeDataObjectMapper nativeMapper = NativeDataObjectMapper.SUPPLIER().create();

    public UserReducer(UserFunction function, ContextTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_REDUCER);
        verifyParameterCount(2);
        verify(function.parameters[0].type().equals(function.parameters[1].type()), "Reducer should take two parameters of the same dataType");
        verifyResultType(function.parameters[0].type());
    }

    @Override
    public DataObject apply(Object value1, Object value2) {
        return timeExecutionOf(() -> function.call(nativeMapper.toDataObject(value1), nativeMapper.toDataObject(value2)));
    }
}
