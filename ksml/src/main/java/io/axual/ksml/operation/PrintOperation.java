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

import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserKeyValuePrinter;

public class PrintOperation extends BaseOperation {
    private static final String MAPPER_NAME = "Mapper";
    private final String filename;
    private final String label;
    private final FunctionDefinition mapper;

    public PrintOperation(OperationConfig config, String filename, String label, FunctionDefinition mapper) {
        super(config);
        this.filename = filename;
        this.label = label;
        this.mapper = mapper;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    void print(
         *          final Printed<K, V> printed)
         */

        final var k = input.keyType();
        final var v = input.valueType();
        final var map = userFunctionOf(context, MAPPER_NAME, mapper, UserKeyValuePrinter.EXPECTED_RESULT_TYPE, superOf(k), superOf(v));
        final var userMap = map != null ? new UserKeyValuePrinter(map, tags) : null;
        final var printed = printedOf(filename, label, userMap);
        input.stream.print(printed);
        return null;
    }
}
