package io.axual.ksml.dsl;

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


public class KSMLDSL {
    private KSMLDSL() {
    }

    public static final String AGGREGATE_INITIALIZER_ATTRIBUTE = "initializer";
    public static final String AGGREGATE_AGGREGATOR_ATTRIBUTE = "aggregator";
    public static final String AGGREGATE_MERGER_ATTRIBUTE = "merger";
    public static final String AGGREGATE_ADDER_ATTRIBTUE = "adder";
    public static final String AGGREGATE_SUBTRACTOR_ATTRIBUTE = "subtractor";
    public static final String BRANCH_PREDICATE_ATTRIBUTE = "if";
    public static final String CONVERT_INTO_ATTRIBUTE = "into";
    public static final String FUNCTION_PARAMETERS_ATTRIBUTE = "parameters";
    public static final String FUNCTION_CODE_ATTRIBUTE = "code";
    public static final String FUNCTION_GLOBALCODE_ATTRIBUTE = "globalCode";
    public static final String FUNCTION_RESULTTYPE_ATTRIBUTE = "resultType";
    public static final String FUNCTION_EXPRESSION_ATTRIBUTE = "expression";
    public static final String FILTER_PREDICATE_ATTRIBUTE = "predicate";
    public static final String FILTERNOT_PREDICATE_ATTRIBUTE = "predicate";
    public static final String STORE_NAME_ATTRIBUTE = "storeName";
    public static final String NAME_ATTRIBUTE = "name";
    public static final String TOPIC_ATTRIBUTE = "topic";
    public static final String KEYTYPE_ATTRIBUTE = "keyType";
    public static final String VALUETYPE_ATTRIBUTE = "valueType";
    public static final String GROUPBY_MAPPER_ATTRIBUTE = "mapper";
    public static final String JOIN_VALUEJOINER_ATTRIBUTE = "valueJoiner";
    public static final String JOIN_MAPPER_ATTRIBUTE = "mapper";
    public static final String JOIN_WINDOW_ATTRIBUTE = "window";
    public static final String MERGE_STREAM_ATTRIBUTE = "stream";
    public static final String PEEK_FOREACH_ATTRIBUTE = "forEach";
    public static final String PIPELINE_BRANCH_ATTRIBUTE = "branch";
    public static final String PIPELINE_TO_ATTRIBUTE = "to";
    public static final String PIPELINE_FOREACH_ATTRIBUTE = "forEach";
    public static final String PIPELINE_TOTOPICNAMEEXTRACTOR_ATTRIBUTE = "toExtractor";
    public static final String REDUCE_REDUCER_ATTRIBUTE = "reducer";
    public static final String REDUCE_ADDER_ATTRIBUTE = "adder";
    public static final String REDUCE_SUBTRACTOR_ATTRIBUTE = "subtractor";
    public static final String PIPELINE_OPERATIONTYPE_ATTRIBUTE = "type";
    public static final String OPERATION_AGGREGATE_TYPE = "aggregate";
    public static final String OPERATION_CONVERTKEY_TYPE = "convertKey";
    public static final String OPERATION_CONVERTVALUE_TYPE = "convertValue";
    public static final String OPERATION_COUNT_TYPE = "count";
    public static final String OPERATION_FILTER_TYPE = "filter";
    public static final String OPERATION_FILTERNOT_TYPE = "filterNot";
    public static final String OPERATION_FLATMAP_TYPE = "flatMap";
    public static final String OPERATION_TRANSFORMKEYVALUETOKEYVALUELIST_TYPE = "transformKeyValueToKeyValueList";
    public static final String OPERATION_FLATMAPVALUES_TYPE = "flatMapValues";
    public static final String OPERATION_TRANSFORMKEYVALUETOVALUELIST_TYPE = "transformKeyValueToValueList";
    public static final String OPERATION_GROUPBY_TYPE = "groupBy";
    public static final String OPERATION_GROUPBYKEY_TYPE = "groupByKey";
    public static final String OPERATION_JOIN_TYPE = "join";
    public static final String OPERATION_LEFTJOIN_TYPE = "leftJoin";
    public static final String OPERATION_MAPKEY_TYPE = "mapKey";
    public static final String OPERATION_SELECTKEY_TYPE = "selectKey";
    public static final String OPERATION_TRANSFORMKEY_TYPE = "transformKey";
    public static final String OPERATION_MAP_TYPE = "map";
    public static final String OPERATION_MAPKEYVALUE_TYPE = "mapKeyValue";
    public static final String OPERATION_TRANSFORMKEYVALUE_TYPE = "transformKeyValue";
    public static final String OPERATION_MAPVALUE_TYPE = "mapValue";
    public static final String OPERATION_MAPVALUES_TYPE = "mapValues";
    public static final String OPERATION_TRANSFORMVALUE_TYPE = "transformValue";
    public static final String OPERATION_MERGE_TYPE = "merge";
    public static final String OPERATION_OUTERJOIN_TYPE = "outerJoin";
    public static final String OPERATION_PEEK_TYPE = "peek";
    public static final String OPERATION_REDUCE_TYPE = "reduce";
    public static final String OPERATION_REPARTITION_TYPE = "repartition";
    public static final String OPERATION_SUPPRESS_TYPE = "suppress";
    public static final String OPERATION_TOSTREAM_TYPE = "toStream";
    public static final String OPERATION_WINDOWEDBY_TYPE = "windowedBy";
    public static final String REPARTITION_PARTITIONER_ATTRIBUTE = "partitioner";
    public static final String SUPPRESS_UNTIL_ATTRIBUTE = "until";
    public static final String SUPPRESS_UNTILTIMELIMIT = "timeLimit";
    public static final String SUPPRESS_UNTILWINDOWCLOSES = "windowCloses";
    public static final String SUPPRESS_DURATION_ATTRIBUTE = "duration";
    public static final String SUPPRESS_BUFFER_MAXBYTES = "maxBytes";
    public static final String SUPPRESS_BUFFER_MAXRECORDS = "maxRecords";
    public static final String SUPPRESS_BUFFERFULLSTRATEGY = "bufferFullStrategy";
    public static final String SUPPRESS_BUFFERFULLSTRATEGY_EMIT = "emitEarlyWhenFull";
    public static final String SUPPRESS_BUFFERFULLSTRATEGY_SHUTDOWN = "shutdownWhenFull";
    public static final String TRANSFORMKEY_MAPPER_ATTRIBUTE = "mapper";
    public static final String TRANSFORMKEYVALUE_MAPPER_ATTRIBUTE = "mapper";
    public static final String TRANSFORMKEYVALUETOKEYVALUELIST_MAPPER_ATTRIBUTE = "mapper";
    public static final String TRANSFORMKEYVALUETOVALUELIST_MAPPER_ATTRIBUTE = "mapper";
    public static final String TRANSFORMVALUE_MAPPER_ATTRIBUTE = "mapper";
    public static final String FUNCTION_PARAMETER_NAME = "name";
    public static final String FUNCTION_PARAMETER_TYPE = "type";
    public static final String FUNCTION_TYPE = "type";
    public static final String FUNCTION_TYPE_AGGREGATOR = "aggregator";
    public static final String FUNCTION_TYPE_FOREACHACTION = "forEach";
    public static final String FUNCTION_TYPE_INITIALIZER = "initializer";
    public static final String FUNCTION_TYPE_KEYTRANSFORMER = "keyTransformer";
    public static final String FUNCTION_TYPE_KEYVALUEMAPPER = "keyValueMapper";
    public static final String FUNCTION_TYPE_KEYVALUETOKEYVALUELISTTRANSFORMER = "keyValueToKeyValueListTransformer";
    public static final String FUNCTION_TYPE_KEYVALUETOVALUELISTTRANSFORMER = "keyValueToValueListTransformer";
    public static final String FUNCTION_TYPE_KEYVALUETRANSFORMER = "keyValueTransformer";
    public static final String FUNCTION_TYPE_MERGER = "merger";
    public static final String FUNCTION_TYPE_PREDICATE = "predicate";
    public static final String FUNCTION_TYPE_REDUCER = "reducer";
    public static final String FUNCTION_TYPE_STREAMPARTITIONER = "streamPartitioner";
    public static final String FUNCTION_TYPE_TOPICNAMEEXTRACTOR = "topicNameExtractor";
    public static final String FUNCTION_TYPE_VALUEJOINER = "valueJoiner";
    public static final String FUNCTION_TYPE_VALUETRANSFORMER = "valueTransformer";
    public static final String WINDOWEDBY_WINDOWTYPE_ATTRIBUTE = "windowType";
    public static final String WINDOWEDBY_WINDOWTYPE_SESSION = "session";
    public static final String WINDOWEDBY_WINDOWTYPE_SLIDING = "sliding";
    public static final String WINDOWEDBY_WINDOWTYPE_TIME = "time";
    public static final String WINDOWEDBY_WINDOWTYPE_SESSION_INACTIVITYGAP = "inactivityGap";
    public static final String WINDOWEDBY_WINDOWTYPE_SESSION_GRACE = "grace";
    public static final String WINDOWEDBY_WINDOWTYPE_SLIDING_TIMEDIFFERENCE = "timeDifference";
    public static final String WINDOWEDBY_WINDOWTYPE_SLIDING_GRACE = "grace";
    public static final String WINDOWEDBY_WINDOWTYPE_TIME_DURATION = "duration";
    public static final String WINDOWEDBY_WINDOWTYPE_TIME_ADVANCEBY = "advanceBy";
    public static final String WINDOWEDBY_WINDOWTYPE_TIME_GRACE = "grace";
    public static final String PIPELINE_FROM_ATTRIBUTE = "from";
    public static final String PIPELINE_VIA_ATTRIBUTE = "via";
    public static final String STREAM_DEFINITION = "stream";
    public static final String STREAMS_DEFINITION = "streams";
    public static final String TABLE_DEFINITION = "table";
    public static final String TABLES_DEFINITION = "tables";
    public static final String GLOBALTABLE_DEFINITION = "globalTable";
    public static final String GLOBALTABLES_DEFINITION = "globalTables";
    public static final String FUNCTIONS_DEFINITION = "functions";
    public static final String PIPELINES_DEFINITION = "pipelines";
}
