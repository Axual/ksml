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
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class UserKeyTransformer extends Invoker implements KeyValueMapper<Object, Object, Object> {
    private final NativeDataObjectMapper nativeMapper = NativeDataObjectMapper.SUPPLIER().create();
    private final static DataType EXPECTED_RESULT_TYPE = DataType.UNKNOWN;

    public UserKeyTransformer(UserFunction function, ContextTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_KEYTRANSFORMER);
        verifyParameterCount(2);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    @Override
    public DataObject apply(Object key, Object value) {
        verifyNoStoresUsed();
        return apply(null, key, value);
    }

    public DataObject apply(StateStores stores, Object key, Object value) {
        return time(() -> function.call(stores, nativeMapper.toDataObject(key), nativeMapper.toDataObject(value)));
    }
}
