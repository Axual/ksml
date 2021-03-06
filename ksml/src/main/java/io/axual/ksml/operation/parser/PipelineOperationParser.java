package io.axual.ksml.operation.parser;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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
import io.axual.ksml.parser.BaseParser;
import io.axual.ksml.parser.ContextAwareParser;
import io.axual.ksml.parser.ParseContext;
import io.axual.ksml.parser.StreamOperation;
import io.axual.ksml.parser.YamlNode;

import static io.axual.ksml.dsl.KSMLDSL.NAME_ATTRIBUTE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_AGGREGATE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_CONVERTKEY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_CONVERTVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_COUNT_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FILTERNOT_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FILTER_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FLATMAPVALUES_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FLATMAP_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_GROUPBYKEY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_GROUPBY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_JOIN_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_LEFTJOIN_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAPKEYVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAPKEY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAPVALUES_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAP_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_SELECTKEY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAPVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MERGE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_OUTERJOIN_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_PEEK_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_REDUCE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_REPARTITION_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_SUPPRESS_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TOSTREAM_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEYVALUETOKEYVALUELIST_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEYVALUETOVALUELIST_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEYVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_WINDOWEDBY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_OPERATIONTYPE_ATTRIBUTE;

public class PipelineOperationParser extends ContextAwareParser<StreamOperation> {
    public PipelineOperationParser(ParseContext context) {
        super(context);
    }

    @Override
    public StreamOperation parse(YamlNode node) {
        if (node == null) return null;

        final String type = parseText(node, PIPELINE_OPERATIONTYPE_ATTRIBUTE);
        if (type == null) {
            throw new KSMLParseException(node, "Type unspecified");
        }

        final String name = determineName(parseText(node, NAME_ATTRIBUTE), type);

        BaseParser<? extends StreamOperation> parser = getParser(type, name);
        if (parser != null) {
            return parser.parse(node.appendName(type));
        }

        throw new KSMLParseException(node, "Unknown type \"" + type + "\" in pipeline operation " + name);
    }

    private BaseParser<? extends StreamOperation> getParser(String type, String name) {
        switch (type) {
            case OPERATION_AGGREGATE_TYPE:
                return new AggregateOperationParser(name, context);
            case OPERATION_CONVERTKEY_TYPE:
                return new ConvertKeyOperationParser(name, context);
            case OPERATION_CONVERTVALUE_TYPE:
                return new ConvertValueOperationParser(name, context);
            case OPERATION_COUNT_TYPE:
                return new CountOperationParser(name, context);
            case OPERATION_FILTER_TYPE:
                return new FilterOperationParser(name, context);
            case OPERATION_FILTERNOT_TYPE:
                return new FilterNotOperationParser(name, context);
            case OPERATION_FLATMAP_TYPE:
            case OPERATION_TRANSFORMKEYVALUETOKEYVALUELIST_TYPE:
                return new TransformKeyValueToKeyValueListOperationParser(name, context);
            case OPERATION_FLATMAPVALUES_TYPE:
            case OPERATION_TRANSFORMKEYVALUETOVALUELIST_TYPE:
                return new TransformKeyValueToValueListOperationParser(name, context);
            case OPERATION_GROUPBY_TYPE:
                return new GroupByOperationParser(name, context);
            case OPERATION_GROUPBYKEY_TYPE:
                return new GroupByKeyOperationParser(name, context);
            case OPERATION_JOIN_TYPE:
                return new JoinOperationParser(name, context);
            case OPERATION_LEFTJOIN_TYPE:
                return new LeftJoinOperationParser(name, context);
            case OPERATION_MAPKEY_TYPE:
            case OPERATION_SELECTKEY_TYPE:
            case OPERATION_TRANSFORMKEY_TYPE:
                return new TransformKeyOperationParser(name, context);
            case OPERATION_MAP_TYPE:
            case OPERATION_MAPKEYVALUE_TYPE:
            case OPERATION_TRANSFORMKEYVALUE_TYPE:
                return new TransformKeyValueOperationParser(name, context);
            case OPERATION_MAPVALUE_TYPE:
            case OPERATION_MAPVALUES_TYPE:
            case OPERATION_TRANSFORMVALUE_TYPE:
                return new TransformValueOperationParser(name, context);
            case OPERATION_MERGE_TYPE:
                return new MergeOperationParser(name, context);
            case OPERATION_OUTERJOIN_TYPE:
                return new OuterJoinOperationParser(name, context);
            case OPERATION_PEEK_TYPE:
                return new PeekOperationParser(name, context);
            case OPERATION_REDUCE_TYPE:
                return new ReduceOperationParser(name, context);
            case OPERATION_REPARTITION_TYPE:
                return new RepartitionOperationParser(name, context);
            case OPERATION_SUPPRESS_TYPE:
                return new SuppressOperationParser(name,context);
            case OPERATION_TOSTREAM_TYPE:
                return new ToStreamOperationParser(name, context);
            case OPERATION_WINDOWEDBY_TYPE:
                return new WindowedByOperationParser(name, context);
            default:
                return null;
        }
    }
}
