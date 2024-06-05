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

import io.axual.ksml.data.notation.UserTupleType;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.ConvertKeyValueOperation;
import io.axual.ksml.parser.StructParser;

public class ConvertKeyValueOperationParser extends OperationParser<ConvertKeyValueOperation> {
    public ConvertKeyValueOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.CONVERT_KEY_VALUE, resources);
    }

    @Override
    public StructParser<ConvertKeyValueOperation> parser() {
        return structParser(
                ConvertKeyValueOperation.class,
                "",
                "An operation to convert the stream key and value types to other types. Conversion is only syntactic, eg. from Avro to XML.",
                operationTypeField(),
                operationNameField(),
                userTypeField(KSMLDSL.Operations.Convert.INTO, "The tuple type to convert the stream key/value into"),
                (type, name, into, tags) -> {
                    if (into.dataType() instanceof UserTupleType userTupleType && userTupleType.subTypeCount() == 2) {
                        return new ConvertKeyValueOperation(operationConfig(name, tags), userTupleType.getUserType(0), userTupleType.getUserType(1));
                    }
                    throw new TopologyException("The type to convert to should be a tuple consisting of two subtypes. For example '(string,avro:SomeSchema)");
                });
    }
}
