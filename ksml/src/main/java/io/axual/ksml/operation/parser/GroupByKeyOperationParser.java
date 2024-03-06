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
import io.axual.ksml.operation.GroupByKeyOperation;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.store.StoreType;

public class GroupByKeyOperationParser extends StoreOperationParser<GroupByKeyOperation> {
    public GroupByKeyOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.GROUP_BY_KEY, resources);
    }

    public StructParser<GroupByKeyOperation> parser() {
        return structParser(
                GroupByKeyOperation.class,
                "",
                "Operation to group all messages with the same key together",
                operationTypeField(),
                operationNameField(),
                storeField(false, "Materialized view of the grouped stream", StoreType.KEYVALUE_STORE),
                (type, name, store) -> new GroupByKeyOperation(storeOperationConfig(name, store)));
    }
}
