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

import io.axual.ksml.parser.FieldParsers;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.parser.StateStoreDefinitionParser;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.operation.DualStoreOperationConfig;
import io.axual.ksml.operation.OperationConfig;
import io.axual.ksml.operation.StoreOperationConfig;
import io.axual.ksml.parser.DefinitionParser;
import io.axual.ksml.parser.NamedObjectParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.StructsParser;
import io.axual.ksml.parser.TopologyResourceFields;
import io.axual.ksml.parser.TopologyResourceParser;
import io.axual.ksml.store.StoreType;
import lombok.Getter;

import java.util.List;
import java.util.function.BiFunction;

@Getter
public abstract class OperationParser<T extends BaseOperation> extends DefinitionParser<T> implements NamedObjectParser {
    private final TopologyResourceFields resourceFields;
    private String defaultShortName;
    private String defaultLongName;
    protected final String type;

    protected OperationParser(String type, TopologyResources resources) {
        this.resourceFields = new TopologyResourceFields(resources);
        this.type = type;
    }

    protected TopologyResources resources() {
        return resourceFields.resources();
    }

    protected <F extends FunctionDefinition> StructsParser<FunctionDefinition> functionField(String childName, String doc, StructsParser<F> parser) {
        return resourceFields.functionField(childName, doc, parser);
    }

    protected StructsParser<TopicDefinition> topicField(String childName, String doc, DefinitionParser<? extends TopicDefinition> parser) {
        return resourceFields.topicField(childName, doc, parser);
    }

    protected <S> StructsParser<S> lookupField(String resourceType, String childName, String doc, BiFunction<String, MetricTags, S> lookup, DefinitionParser<? extends S> parser) {
        return resourceFields.lookupField(resourceType, childName, doc, lookup, parser);
    }

    protected StructsParser<String> operationNameField() {
        final var parser =  FieldParsers.optional(FieldParsers.stringField(KSMLDSL.Operations.NAME_ATTRIBUTE, false, "The name of the operation processor"));
        return new StructsParser<>() {
            @Override
            public String parse(ParseNode node) {
                return parser.parse(node);
            }

            @Override
            public List<StructSchema> schemas() {
                return parser.schemas();
            }
        };
    }

    protected OperationConfig operationConfig(String name, MetricTags tags) {
        name = FieldParsers.validateName("Operation", name, defaultLongName != null ? defaultLongName + "_" + type : type);
        return new OperationConfig(
                name != null ? resources().getUniqueOperationName(name) : resources().getUniqueOperationName(tags),
                tags);
    }

    protected StoreOperationConfig storeOperationConfig(String name, MetricTags tags, StateStoreDefinition store) {
        name = FieldParsers.validateName("Store", name, defaultShortName(), true);
        return new StoreOperationConfig(name != null ? resources().getUniqueOperationName(name) : resources().getUniqueOperationName(tags), tags, store);
    }

    protected DualStoreOperationConfig dualStoreOperationConfig(String name, MetricTags tags, StateStoreDefinition store1, StateStoreDefinition store2) {
        name = FieldParsers.validateName("Store", name, defaultShortName(), true);
        return new DualStoreOperationConfig(name != null ? resources().getUniqueOperationName(name) : resources().getUniqueOperationName(tags), tags, store1, store2);
    }

    protected StructsParser<StateStoreDefinition> storeField(boolean required, String doc, StoreType expectedStoreType) {
        return storeField(KSMLDSL.Operations.STORE_ATTRIBUTE, required, doc, expectedStoreType);
    }

    protected StructsParser<StateStoreDefinition> storeField(String childName, boolean required, String doc, StoreType expectedStoreType) {
        final var stateStoreParser = new StateStoreDefinitionParser(expectedStoreType, false);
        final var resourceParser = new TopologyResourceParser<>("state store", childName, doc, (name, context) -> resources().stateStore(name), stateStoreParser);
        final var schemas = required ? resourceParser.schemas() : FieldParsers.optional(resourceParser).schemas();
        return new StructsParser<>() {
            @Override
            public StateStoreDefinition parse(ParseNode node) {
                stateStoreParser.defaultShortName(node.name());
                stateStoreParser.defaultLongName(node.longName());
                final var resource = resourceParser.parse(node);
                if (resource != null && resource.definition() instanceof StateStoreDefinition def) return def;
                if (!required) return null;
                throw new ParseException(node, "Required state store is not defined");
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
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
