package io.axual.ksml.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.UnresolvedType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.TopologyResource;
import io.axual.ksml.exception.TopologyException;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.generator.TopologyBaseResources;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.type.UserType;

import java.util.List;
import java.util.function.BiFunction;

// Field-builder helpers available while only the base resource set (functions and state stores)
// is known — i.e. while streams/tables/topics are themselves still being defined, and cannot yet
// be referenced by name. Composed into (not extended by) parsers that operate at this phase; see
// TopologyResourceFields for the phase where the full resource set, including topics, is available.
public class TopologyBaseResourceFields {
    private final TopologyBaseResources resources;

    public TopologyBaseResourceFields(TopologyBaseResources resources) {
        this.resources = resources;
    }

    public TopologyBaseResources resources() {
        if (resources != null) return resources;
        throw new TopologyException("Topology base resources not properly initialized. This is a programming error.");
    }

    public <F extends FunctionDefinition> StructsParser<FunctionDefinition> functionField(String childName, String doc, StructsParser<F> parser) {
        final var resourceParser = new TopologyResourceParser<>("function", childName, doc, (name, tags) -> resources.function(name), parser);
        return StructsParser.of(resourceParser::parseDefinition, resourceParser.schemas());
    }

    public <S> StructsParser<S> lookupField(String resourceType, String childName, String doc, BiFunction<String, MetricTags, S> lookup, DefinitionParser<? extends S> parser) {
        final var resourceParser = new TopologyResourceParser<>(resourceType, childName, doc, lookup, parser);
        final var schemas = resourceParser.schemas();
        return new StructsParser<>() {
            @Override
            public S parse(ParseNode node) {
                if (node == null) return null;
                final var resource = resourceParser.parse(node);
                return (resource != null) ? resource.definition() : null;
            }

            @Override
            public List<StructSchema> schemas() {
                return schemas;
            }
        };
    }

    public <S> StructsParser<TopologyResource<S>> topologyResourceField(String resourceType, String childName, String doc, BiFunction<String, MetricTags, S> lookup, DefinitionParser<S> parser) {
        return new TopologyResourceParser<>(resourceType, childName, doc, lookup, parser, true);
    }

    /**
     * Resolves a UserType that contains an {@link UnresolvedType} by fetching the schema
     * from the remote schema registry using the topic name and key/value suffix.
     *
     * @param userType the UserType to resolve (returned as-is if already resolved)
     * @param topic    the Kafka topic name
     * @param isKey    whether this is a key type (true) or value type (false)
     * @return the resolved UserType with a concrete DataType
     * @throws SchemaException if the notation is unknown or the schema cannot be fetched
     */
    public static UserType resolveUserType(UserType userType, String topic, boolean isKey) {
        if (userType == null || !(userType.dataType() instanceof UnresolvedType)) {
            return userType;
        }
        final var notation = ExecutionContext.INSTANCE.notationLibrary().get(userType.notation());
        if (notation == null) {
            throw new SchemaException("Unknown notation: " + userType.notation());
        }
        final var schema = ExecutionContext.INSTANCE.schemaLibrary().getOrFetchRemoteSchema(notation, topic, isKey);
        if (schema == null) {
            throw new SchemaException("Notation '" + userType.notation() + "' does not support fetching schemas from a remote registry");
        }
        final var dataType = new DataTypeDataSchemaMapper().fromDataSchema(schema);
        return new UserType(userType.notation(), dataType);
    }
}
