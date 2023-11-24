package io.axual.ksml.operation;

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
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.UserType;
import io.axual.ksml.operation.processor.OperationProcessorSupplier;
import io.axual.ksml.operation.processor.TransformKeyValueToValueListProcessor;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserKeyValueToValueListTransformer;
import org.apache.kafka.streams.kstream.Named;

public class TransformKeyValueToValueListOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final UserFunction mapper;

    public TransformKeyValueToValueListOperation(OperationConfig config, UserFunction mapper) {
        super(config);
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input) {
        checkNotNull(mapper, MAPPER_NAME.toLowerCase());
        final var k = input.keyType();
        final var v = input.valueType();
        final var vr = streamDataTypeOf(firstSpecificType(mapper, new UserType(new ListType(v.userType().dataType()))), false);
        final var mapperResultType = new UserType(new ListType(DataType.UNKNOWN));
        checkFunction(MAPPER_NAME, mapper, subOf(mapperResultType), vr, superOf(k), superOf(v));

        final var action = new UserKeyValueToValueListTransformer(mapper);
        final var storeNames = combineStoreNames(this.storeNames, mapper.storeNames);
        final var supplier = new OperationProcessorSupplier<>(
                name,
                TransformKeyValueToValueListProcessor::new,
                (stores, record) -> action.apply(stores, record.key(), record.value()),
                storeNames);
        final var output = name != null
                ? input.stream.process(supplier, Named.as(name), storeNames)
                : input.stream.process(supplier, storeNames);
        return new KStreamWrapper(output, k, vr);
    }
}
