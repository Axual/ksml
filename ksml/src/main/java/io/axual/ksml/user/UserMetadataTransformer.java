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
import io.axual.ksml.data.mapper.RecordMetadataDataObjectMapper;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.RecordMetadata;
import io.axual.ksml.definition.DefinitionConstants;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.Invoker;
import io.axual.ksml.store.StateStores;

public class UserMetadataTransformer extends Invoker {
    public static final DataType EXPECTED_RESULT_TYPE = DefinitionConstants.METADATA_TYPE;
    private static final RecordMetadataDataObjectMapper META_MAPPER = new RecordMetadataDataObjectMapper();
    private static final NativeDataObjectMapper NATIVE_MAPPER = new DataObjectFlattener();

    public UserMetadataTransformer(UserFunction function, MetricTags tags) {
        super(function, tags, KSMLDSL.Functions.TYPE_METADATATRANSFORMER);
        verifyParameterCount(3);
        verifyResultType(EXPECTED_RESULT_TYPE);
    }

    public RecordMetadata apply(StateStores stores, Object key, Object value, RecordMetadata metadata) {
        final var resultMeta = function.call(stores, NATIVE_MAPPER.toDataObject(key), NATIVE_MAPPER.toDataObject(value), META_MAPPER.toDataObject(metadata));
        return timeExecutionOf(() -> META_MAPPER.fromDataObject(resultMeta));
    }
}
