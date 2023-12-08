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


import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.generator.TopologyResources;
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.operation.StreamOperation;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.*;

public class PipelineOperationParser extends ContextAwareParser<StreamOperation> {
    public PipelineOperationParser(String prefix, TopologyResources resources) {
        super(prefix, resources);
    }

    @Override
    public StreamOperation parse(YamlNode node) {
        if (node == null) return null;

        final String type = parseString(node, Pipelines.OPERATION_TYPE);
        if (type == null) {
            throw new KSMLParseException(node, "Type unspecified");
        }

        final String name = determineName(parseString(node, Operations.NAME_ATTRIBUTE), type);

        BaseParser<? extends StreamOperation> parser = getParser(type, name);
        if (parser != null) {
            return parser.parse(node.appendName(type));
        }

        throw new KSMLParseException(node, "Unknown dataType \"" + type + "\" in pipeline operation " + name);
    }

    private BaseParser<? extends StreamOperation> getParser(String type, String name) {
        return switch (type) {
            case Operations.AS -> new AsOperationParser(prefix, name, resources);
            case Operations.AGGREGATE -> new AggregateOperationParser(prefix, name, resources);
            case Operations.COGROUP -> new CogroupOperationParser(prefix, name, resources);
            case Operations.CONVERT_KEY -> new ConvertKeyOperationParser(prefix, name, resources);
            case Operations.CONVERT_KEY_VALUE -> new ConvertKeyValueOperationParser(prefix, name, resources);
            case Operations.CONVERT_VALUE -> new ConvertValueOperationParser(prefix, name, resources);
            case Operations.COUNT -> new CountOperationParser(prefix, name, resources);
            case Operations.FILTER -> new FilterOperationParser(prefix, name, resources);
            case Operations.FILTER_NOT -> new FilterNotOperationParser(prefix, name, resources);
            case Operations.FLATMAP, Operations.TRANSFORM_KEY_VALUE_TO_KEY_VALUE_LIST ->
                    new TransformKeyValueToKeyValueListOperationParser(prefix, name, resources);
            case Operations.FLATMAP_VALUES, Operations.TRANSFORM_KEY_VALUE_TO_VALUE_LIST ->
                    new TransformKeyValueToValueListOperationParser(prefix, name, resources);
            case Operations.GROUP_BY -> new GroupByOperationParser(prefix, name, resources);
            case Operations.GROUP_BY_KEY -> new GroupByKeyOperationParser(prefix, name, resources);
            case Operations.JOIN -> new JoinOperationParser(prefix, name, resources);
            case Operations.LEFT_JOIN -> new LeftJoinOperationParser(prefix, name, resources);
            case Operations.MAP_KEY, Operations.SELECT_KEY, Operations.TRANSFORM_KEY ->
                    new TransformKeyOperationParser(prefix, name, resources);
            case Operations.MAP, Operations.MAP_KEY_VALUE, Operations.TRANSFORM_KEY_VALUE ->
                    new TransformKeyValueOperationParser(prefix, name, resources);
            case Operations.MAP_VALUE, Operations.MAP_VALUES, Operations.TRANSFORM_VALUE ->
                    new TransformValueOperationParser(prefix, name, resources);
            case Operations.MERGE -> new MergeOperationParser(prefix, name, resources);
            case Operations.OUTER_JOIN -> new OuterJoinOperationParser(prefix, name, resources);
            case Operations.PEEK -> new PeekOperationParser(prefix, name, resources);
            case Operations.REDUCE -> new ReduceOperationParser(prefix, name, resources);
            case Operations.REPARTITION -> new RepartitionOperationParser(prefix, name, resources);
            case Operations.SUPPRESS -> new SuppressOperationParser(prefix, name, resources);
            case Operations.TO_STREAM -> new ToStreamOperationParser(prefix, name, resources);
            case Operations.TO_TABLE -> new ToTableOperationParser(prefix, name, resources);
            case Operations.WINDOW_BY_TIME -> new WindowByTimeOperationParser(prefix, name, resources);
            case Operations.WINDOW_BY_SESSION -> new WindowBySessionOperationParser(prefix, name, resources);
            default -> null;
        };
    }
}
