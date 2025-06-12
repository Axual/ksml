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
import io.axual.ksml.operation.ToTableOperation;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.store.StoreType;

public class ToTableOperationParser extends OperationParser<ToTableOperation> {
    public ToTableOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.TO_TABLE, resources);
    }

    @Override
    public StructsParser<ToTableOperation> parser() {
        return structsParser(
                ToTableOperation.class,
                "",
                "Convert a Stream into a Table",
                operationNameField(),
                storeField(false, "Materialized view of the result table", StoreType.KEYVALUE_STORE),
                (name, store, tags) -> new ToTableOperation(storeOperationConfig(name, tags, store)));
    }
}
