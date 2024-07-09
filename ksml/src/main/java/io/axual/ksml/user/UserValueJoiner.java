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
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.tag.ContextTags;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.python.Invoker;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class UserValueJoiner extends Invoker implements ValueJoiner<Object, Object, Object> {
    private static final DataType EXPECTED_RESULT_TYPE = DataType.UNKNOWN;
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();

    public UserValueJoiner(UserFunction function, ContextTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_VALUEJOINER);
        verifyParameterCount(3);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public DataObject apply(Object value1, Object value2) {
        // ValueJoiners in KSML are always defined as ValueJoinerWithKey, meaning they take a key and two value
        // parameters. Since we are calling from a traditional ValueJoiner, we pass in a NULL key to the function.
        return timeExecutionOf(() -> function.call(DataNull.INSTANCE, NATIVE_MAPPER.toDataObject(value1), NATIVE_MAPPER.toDataObject(value2)));
    }
}
