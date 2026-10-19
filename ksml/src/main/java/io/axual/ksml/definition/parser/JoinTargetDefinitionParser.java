package io.axual.ksml.definition.parser;

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


import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.exception.ParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ParseNode;
import io.axual.ksml.parser.TopologyResourceParser;

import static io.axual.ksml.dsl.KSMLDSL.Operations;

public class JoinTargetDefinitionParser extends BaseParser<TopologyResource<TopicDefinition>> {
    private final TopologyResources resources;

    public JoinTargetDefinitionParser(TopologyResources resources) {
        this.resources = resources;
    }

    @Override
    public TopologyResource<TopicDefinition> parse(ParseNode node) {
        if (node == null) return null;
        // A stream or table that is not defined can be saved with "as" in an earlier pipeline, so it is looked up by
        // name when building
        if (node.get(Operations.Join.WITH_STREAM) != null) {
            final var result = new TopologyResourceParser<>("stream", Operations.Join.WITH_STREAM, null, (name, tags) -> resources.topic(name), new StreamDefinitionParser(resources, true), true).parse(node);
            return requireDefinedOrSaved(node, resources, result, "stream", "streams");
        }
        if (node.get(Operations.Join.WITH_TABLE) != null) {
            final var result = new TopologyResourceParser<>("table", Operations.Join.WITH_TABLE, null, (name, tags) -> resources.topic(name), new TableDefinitionParser(resources, true), true).parse(node);
            return requireDefinedOrSaved(node, resources, result, "table", "tables");
        }
        if (node.get(Operations.Join.WITH_GLOBAL_TABLE) != null) {
            final var result = new TopologyResourceParser<>("globalTable", Operations.Join.WITH_GLOBAL_TABLE, null, (name, tags) -> resources.topic(name), new GlobalTableDefinitionParser(resources, true), true).parse(node);
            // Kafka Streams can not make a GlobalKTable from a pipeline result, so only defined globalTables work
            if (result.definition() == null) {
                throw new ParseException(node, "Unknown globalTable \"" + result.name() + "\". Define it under \"globalTables\". A stream or table saved with \"as\" can not be used as globalTable, join with it using \"table\" instead");
            }
            return result;
        }
        throw new ParseException(node, "Stream definition missing");
    }

    // A name that is not defined must be saved by an earlier pipeline, the same rule as when building. Checking it
    // here gives typos the YAML location
    public static <T extends TopicDefinition> TopologyResource<T> requireDefinedOrSaved(ParseNode node, TopologyResources resources, TopologyResource<T> target, String kind, String section) {
        if (target == null || target.definition() != null || resources.isSavedName(target.name())) return target;
        throw new ParseException(node, "Unknown " + kind + " \"" + target.name() + "\". Define it under \"" + section + "\", or save a " + kind + " with \"as: " + target.name() + "\" in a pipeline defined before this one, in the same definition file");
    }
}
