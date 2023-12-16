package io.axual.ksml.operation.parser;

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

import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ConvertValueOperation;
import io.axual.ksml.parser.StructParser;

public class ConvertValueOperationParser extends OperationParser<ConvertValueOperation> {
    public ConvertValueOperationParser(TopologyResources resources) {
        super("convertValue", resources);
    }

    public StructParser<ConvertValueOperation> parser() {
        return structParser(
                ConvertValueOperation.class,
                "An operation to convert the stream value type to another type. Conversion is only syntactic, eg. from Avro to XML.",
                operationTypeField(KSMLDSL.Operations.CONVERT_VALUE),
                nameField(),
                userTypeField(KSMLDSL.Operations.Convert.INTO, true, "The type to convert the stream value into"),
                (type, name, into) -> new ConvertValueOperation(operationConfig(name), into));
    }
}
