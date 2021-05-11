package io.axual.ksml.parser;

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

import static io.axual.ksml.dsl.KSMLDSL.OPERATION_AGGREGATE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_COUNT_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FILTERNOT_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FILTER_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FLATMAPVALUES_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_FLATMAP_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_GROUPBY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_JOIN_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_LEFTJOIN_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAPKEYVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAPKEY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MAPVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_MERGE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_OUTERJOIN_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_PEEK_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_REDUCE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_REPARTITION_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TOSTREAM_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEYVALUETOKEYVALUELIST_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEYVALUETOVALUELIST_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEYVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMKEY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_TRANSFORMVALUE_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.OPERATION_WINDOWEDBY_TYPE;
import static io.axual.ksml.dsl.KSMLDSL.PIPELINE_OPERATIONTYPE_ATTRIBUTE;

public class PipelineOperationParser extends ContextAwareParser<StreamOperation> {
    protected PipelineOperationParser(ParseContext context) {
        super(context);
    }

    @Override
    public StreamOperation parse(YamlNode node) {
        if (node == null) return null;

        final String type = parseText(node, PIPELINE_OPERATIONTYPE_ATTRIBUTE);
        if (type == null) {
            throw new KSMLParseException(node, "Type unspecified");
        }

        BaseParser<? extends StreamOperation> parser = getParser(type);
        if (parser != null) {
            return parser.parse(node.appendName(type));
        }

        throw new KSMLParseException(node, "Unknown type \"" + type + "\" in pipeline operation");
    }

    private BaseParser<? extends StreamOperation> getParser(String type) {
        switch (type) {
            case OPERATION_AGGREGATE_TYPE:
                return new AggregateOperationParser(context);
            case OPERATION_COUNT_TYPE:
                return new CountOperationParser(context);
            case OPERATION_FILTER_TYPE:
                return new FilterOperationParser(context);
            case OPERATION_FILTERNOT_TYPE:
                return new FilterNotOperationParser(context);
            case OPERATION_FLATMAP_TYPE:
            case OPERATION_TRANSFORMKEYVALUETOKEYVALUELIST_TYPE:
                return new TransformKeyValueToKeyValueListOperationParser(context);
            case OPERATION_FLATMAPVALUES_TYPE:
            case OPERATION_TRANSFORMKEYVALUETOVALUELIST_TYPE:
                return new TransformKeyValueToValueListOperationParser(context);
            case OPERATION_GROUPBY_TYPE:
                return new GroupByOperationParser(context);
            case OPERATION_JOIN_TYPE:
                return new JoinOperationParser(context);
            case OPERATION_LEFTJOIN_TYPE:
                return new LeftJoinOperationParser(context);
            case OPERATION_MAPKEY_TYPE:
            case OPERATION_TRANSFORMKEY_TYPE:
                return new TransformKeyOperationParser(context);
            case OPERATION_MAPKEYVALUE_TYPE:
            case OPERATION_TRANSFORMKEYVALUE_TYPE:
                return new TransformKeyValueOperationParser(context);
            case OPERATION_MAPVALUE_TYPE:
            case OPERATION_TRANSFORMVALUE_TYPE:
                return new TransformValueOperationParser(context);
            case OPERATION_MERGE_TYPE:
                return new MergeOperationParser(context);
            case OPERATION_OUTERJOIN_TYPE:
                return new OuterJoinOperationParser(context);
            case OPERATION_PEEK_TYPE:
                return new PeekOperationParser(context);
            case OPERATION_REDUCE_TYPE:
                return new ReduceOperationParser(context);
            case OPERATION_REPARTITION_TYPE:
                return new RepartitionOperationParser(context);
            case OPERATION_TOSTREAM_TYPE:
                return new ToStreamOperationParser(context);
            case OPERATION_WINDOWEDBY_TYPE:
                return new WindowedByOperationParser(context);
            default:
                return null;
        }
    }
}
