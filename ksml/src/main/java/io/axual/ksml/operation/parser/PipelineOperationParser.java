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
import io.axual.ksml.operation.StreamOperation;
import io.axual.ksml.parser.ChoiceParser;
import io.axual.ksml.parser.StructsParser;

import java.util.HashMap;
import java.util.Map;

public class PipelineOperationParser extends ChoiceParser<StreamOperation> {
    public PipelineOperationParser(TopologyResources resources) {
        super(KSMLDSL.Operations.TYPE_ATTRIBUTE, "OperationType", "operation", null, types(resources));
    }

    private static Map<String, StructsParser<? extends StreamOperation>> types(TopologyResources resources) {
        final var result = new HashMap<String, StructsParser<? extends StreamOperation>>();
        result.put(KSMLDSL.Operations.AGGREGATE, new AggregateOperationParser(resources));
        result.put(KSMLDSL.Operations.COGROUP, new CogroupOperationParser(resources));
        result.put(KSMLDSL.Operations.CONVERT_KEY, new ConvertKeyOperationParser(resources));
        result.put(KSMLDSL.Operations.CONVERT_KEY_VALUE, new ConvertKeyValueOperationParser(resources));
        result.put(KSMLDSL.Operations.CONVERT_VALUE, new ConvertValueOperationParser(resources));
        result.put(KSMLDSL.Operations.COUNT, new CountOperationParser(resources));
        result.put(KSMLDSL.Operations.FILTER, new FilterOperationParser(resources));
        result.put(KSMLDSL.Operations.FILTER_NOT, new FilterNotOperationParser(resources));
        result.put(KSMLDSL.Operations.FLATMAP, new TransformKeyValueToKeyValueListOperationParser(resources));
        result.put(KSMLDSL.Operations.TRANSFORM_KEY_VALUE_TO_KEY_VALUE_LIST, new TransformKeyValueToKeyValueListOperationParser(resources));
        result.put(KSMLDSL.Operations.FLATMAP_VALUES, new TransformKeyValueToValueListOperationParser(resources));
        result.put(KSMLDSL.Operations.TRANSFORM_KEY_VALUE_TO_VALUE_LIST, new TransformKeyValueToValueListOperationParser(resources));
        result.put(KSMLDSL.Operations.GROUP_BY, new GroupByOperationParser(resources));
        result.put(KSMLDSL.Operations.GROUP_BY_KEY, new GroupByKeyOperationParser(resources));
        result.put(KSMLDSL.Operations.JOIN, new JoinOperationParser(resources));
        result.put(KSMLDSL.Operations.LEFT_JOIN, new LeftJoinOperationParser(resources));
        result.put(KSMLDSL.Operations.MAP_KEY, new TransformKeyOperationParser(resources));
        result.put(KSMLDSL.Operations.SELECT_KEY, new TransformKeyOperationParser(resources));
        result.put(KSMLDSL.Operations.TRANSFORM_KEY, new TransformKeyOperationParser(resources));
        result.put(KSMLDSL.Operations.MAP, new TransformKeyValueOperationParser(resources));
        result.put(KSMLDSL.Operations.TRANSFORM_KEY_VALUE, new TransformKeyValueOperationParser(resources));
        result.put(KSMLDSL.Operations.TRANSFORM_METADATA, new TransformMetadataOperationParser(resources));
        result.put(KSMLDSL.Operations.MAP_VALUE, new TransformValueOperationParser(resources));
        result.put(KSMLDSL.Operations.MAP_VALUES, new TransformValueOperationParser(resources));
        result.put(KSMLDSL.Operations.TRANSFORM_VALUE, new TransformValueOperationParser(resources));
        result.put(KSMLDSL.Operations.MERGE, new MergeOperationParser(resources));
        result.put(KSMLDSL.Operations.OUTER_JOIN, new OuterJoinOperationParser(resources));
        result.put(KSMLDSL.Operations.PEEK, new PeekOperationParser(resources));
        result.put(KSMLDSL.Operations.REDUCE, new ReduceOperationParser(resources));
        result.put(KSMLDSL.Operations.REPARTITION, new RepartitionOperationParser(resources));
        result.put(KSMLDSL.Operations.SUPPRESS, new SuppressOperationParser(resources));
        result.put(KSMLDSL.Operations.TO_STREAM, new ToStreamOperationParser(resources));
        result.put(KSMLDSL.Operations.TO_TABLE, new ToTableOperationParser(resources));
        result.put(KSMLDSL.Operations.WINDOW_BY_TIME, new WindowByTimeOperationParser(resources));
        result.put(KSMLDSL.Operations.WINDOW_BY_SESSION, new WindowBySessionOperationParser(resources));
        return result;
    }
}
