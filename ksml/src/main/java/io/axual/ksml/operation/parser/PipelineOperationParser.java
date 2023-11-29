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
    public PipelineOperationParser(TopologyResources resources) {
        super(resources);
    }

    @Override
    public StreamOperation parse(YamlNode node) {
        if (node == null) return null;

        final String type = parseString(node, PIPELINE_OPERATIONTYPE_ATTRIBUTE);
        if (type == null) {
            throw new KSMLParseException(node, "Type unspecified");
        }

        final String name = determineName(parseString(node, NAME_ATTRIBUTE), type);

        BaseParser<? extends StreamOperation> parser = getParser(type, name);
        if (parser != null) {
            return parser.parse(node.appendName(type));
        }

        throw new KSMLParseException(node, "Unknown dataType \"" + type + "\" in pipeline operation " + name);
    }

    private BaseParser<? extends StreamOperation> getParser(String type, String name) {
        return switch (type) {
            case OPERATION_AGGREGATE_TYPE -> new AggregateOperationParser(name, resources);
            case OPERATION_CONVERTKEY_TYPE -> new ConvertKeyOperationParser(name, resources);
            case OPERATION_CONVERTKEYVALUE_TYPE -> new ConvertKeyValueOperationParser(name, resources);
            case OPERATION_CONVERTVALUE_TYPE -> new ConvertValueOperationParser(name, resources);
            case OPERATION_COUNT_TYPE -> new CountOperationParser(name, resources);
            case OPERATION_FILTER_TYPE -> new FilterOperationParser(name, resources);
            case OPERATION_FILTERNOT_TYPE -> new FilterNotOperationParser(name, resources);
            case OPERATION_FLATMAP_TYPE, OPERATION_TRANSFORMKEYVALUETOKEYVALUELIST_TYPE -> new TransformKeyValueToKeyValueListOperationParser(name, resources);
            case OPERATION_FLATMAPVALUES_TYPE, OPERATION_TRANSFORMKEYVALUETOVALUELIST_TYPE -> new TransformKeyValueToValueListOperationParser(name, resources);
            case OPERATION_GROUPBY_TYPE -> new GroupByOperationParser(name, resources);
            case OPERATION_GROUPBYKEY_TYPE -> new GroupByKeyOperationParser(name, resources);
            case OPERATION_JOIN_TYPE -> new JoinOperationParser(name, resources);
            case OPERATION_LEFTJOIN_TYPE -> new LeftJoinOperationParser(name, resources);
            case OPERATION_MAPKEY_TYPE, OPERATION_SELECTKEY_TYPE, OPERATION_TRANSFORMKEY_TYPE -> new TransformKeyOperationParser(name, resources);
            case OPERATION_MAP_TYPE, OPERATION_MAPKEYVALUE_TYPE, OPERATION_TRANSFORMKEYVALUE_TYPE -> new TransformKeyValueOperationParser(name, resources);
            case OPERATION_MAPVALUE_TYPE, OPERATION_MAPVALUES_TYPE, OPERATION_TRANSFORMVALUE_TYPE -> new TransformValueOperationParser(name, resources);
            case OPERATION_MERGE_TYPE -> new MergeOperationParser(name, resources);
            case OPERATION_OUTERJOIN_TYPE -> new OuterJoinOperationParser(name, resources);
            case OPERATION_PEEK_TYPE -> new PeekOperationParser(name, resources);
            case OPERATION_REDUCE_TYPE -> new ReduceOperationParser(name, resources);
            case OPERATION_REPARTITION_TYPE -> new RepartitionOperationParser(name, resources);
            case OPERATION_SUPPRESS_TYPE -> new SuppressOperationParser(name, resources);
            case OPERATION_TOSTREAM_TYPE -> new ToStreamOperationParser(name, resources);
            case OPERATION_WINDOWBYTIME_TYPE -> new WindowByTimeOperationParser(name,resources);
            case OPERATION_WINDOWBYSESSION_TYPE -> new WindowBySessionOperationParser(name,resources);
            default -> null;
        };
    }
}
