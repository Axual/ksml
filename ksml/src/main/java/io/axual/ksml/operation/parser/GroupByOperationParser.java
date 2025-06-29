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


import io.axual.ksml.definition.parser.KeyValueMapperDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.GroupByOperation;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.store.StoreType;

public class GroupByOperationParser extends OperationParser<GroupByOperation> {
    public GroupByOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.GROUP_BY, resources);
    }

    public StructsParser<GroupByOperation> parser() {
        return structsParser(
                GroupByOperation.class,
                "",
                "Operation to group all messages with together based on a keying function",
                operationNameField(),
                functionField(KSMLDSL.Operations.GroupBy.MAPPER, "Function to map records to a key they can be grouped on", new KeyValueMapperDefinitionParser(false)),
                storeField(false, "Materialized view of the grouped stream or table", StoreType.KEYVALUE_STORE),
                (name, mapper, store, tags) -> new GroupByOperation(storeOperationConfig(name, tags, store), mapper));
    }
}
