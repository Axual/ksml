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

import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.dsl.KSMLDSL;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.operation.BaseOperation;
import io.axual.ksml.operation.OperationConfig;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.StringValueParser;
import io.axual.ksml.parser.StructParser;
import io.axual.ksml.parser.YamlNode;

import java.util.List;

public abstract class OperationParser<T extends BaseOperation> extends ContextAwareParser<T> {
    private static final String[] TEMPLATE = new String[0];
    private final String type;

    public OperationParser(String type, TopologyResources resources) {
        super(resources);
        this.type = type;
    }

    protected StructParser<String> nameField() {
        final var stringParser = stringField(KSMLDSL.Operations.NAME_ATTRIBUTE, false, type, "The name of the operation processor");
        return new StructParser<>() {
            @Override
            public String parse(YamlNode node) {
                final var name = stringParser.parse(node);
                // To ensure every operation gets a unique name, we generate one based on the YAML node
                return name != null ? name : node.longName();
            }

            @Override
            public StructSchema schema() {
                return stringParser.schema();
            }
        };
    }

    protected StructParser<List<String>> storeNamesField() {
        return listField(KSMLDSL.Operations.STORE_NAMES_ATTRIBUTE, "state store name", false, "The names of all state stores used by the function", new StringValueParser());
    }

    protected OperationConfig operationConfig(String name) {
        return operationConfig(name, null);
    }

    protected OperationConfig operationConfig(String name, List<String> storeNames) {
        return new OperationConfig(
                namespace(),
                name != null ? determineName(name) : determineName(getClass().getSimpleName()),
                storeNames != null ? storeNames.toArray(TEMPLATE) : null);
    }
}
