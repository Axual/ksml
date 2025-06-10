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

import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.operation.DualStoreOperationConfig;
import io.axual.ksml.operation.OperationConfig;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceAwareParser;
import io.axual.ksml.store.StoreParserUtil;
import io.axual.ksml.store.StoreType;
import lombok.Getter;

@Getter
public abstract class OperationParser<T extends BaseOperation> extends TopologyResourceAwareParser<T> implements NamedObjectParser {
    private String defaultShortName;
    private String defaultLongName;
    protected final String type;

    protected OperationParser(String type, TopologyResources resources) {
        super(resources);
        this.type = type;
    }

    protected StructsParser<String> operationNameField() {
        return optional(stringField(KSMLDSL.Operations.NAME_ATTRIBUTE, false, type, "The name of the operation processor"));
    }

    protected OperationConfig operationConfig(String name, MetricTags tags) {
        name = validateName("Operation", name, defaultLongName != null ? defaultLongName + "_" + type : type);
        return new OperationConfig(
                name != null ? resources().getUniqueOperationName(name) : resources().getUniqueOperationName(tags),
                tags);
    }

    protected StoreOperationConfig storeOperationConfig(String name, MetricTags tags, StateStoreDefinition store) {
        name = validateName("Store", name, defaultShortName(), true);
        return new StoreOperationConfig(name != null ? resources().getUniqueOperationName(name) : resources().getUniqueOperationName(tags), tags, store);
    }

    protected StructsParser<StateStoreDefinition> storeField(boolean required, String doc, StoreType expectedStoreType) {
        return StoreParserUtil.storeField(KSMLDSL.Operations.STORE_ATTRIBUTE, required, doc, expectedStoreType, resources());
    }

    protected DualStoreOperationConfig dualStoreOperationConfig(String name, MetricTags tags, StateStoreDefinition store1, StateStoreDefinition store2) {
        name = validateName("Store", name, defaultShortName(), true);
        return new DualStoreOperationConfig(name != null ? resources().getUniqueOperationName(name) : resources().getUniqueOperationName(tags), tags, store1, store2);
    }

    @Override
    public void defaultShortName(String name) {
        this.defaultShortName = name;
    }

    @Override
    public void defaultLongName(String name) {
        this.defaultLongName = name;
    }
}
