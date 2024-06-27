package io.axual.ksml.dsl;

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


import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KSMLDSL {
    public static final String FUNCTIONS = "functions";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Functions {
        public static final String NAME = "name";
        public static final String PARAMETERS = "parameters";
        public static final String CODE = "code";
        public static final String GLOBAL_CODE = "globalCode";
        public static final String STORES = "stores";
        public static final String RESULT_TYPE = "resultType";
        public static final String EXPRESSION = "expression";

        public static final String TYPE = "type";
        public static final String TYPE_AGGREGATOR = "aggregator";
        public static final String TYPE_FOREACHACTION = "forEach";
        public static final String TYPE_FOREIGN_KEY_EXTRACTOR = "foreignKeyExtractor";
        public static final String TYPE_GENERATOR = "generator";
        public static final String TYPE_GENERIC = "generic";
        public static final String TYPE_INITIALIZER = "initializer";
        public static final String TYPE_KEYTRANSFORMER = "keyTransformer";
        public static final String TYPE_KEYVALUEMAPPER = "keyValueMapper";
        public static final String TYPE_KEYVALUEPRINTER = "keyValuePrinter";
        public static final String TYPE_KEYVALUETOKEYVALUELISTTRANSFORMER = "keyValueToKeyValueListTransformer";
        public static final String TYPE_KEYVALUETOVALUELISTTRANSFORMER = "keyValueToValueListTransformer";
        public static final String TYPE_KEYVALUETRANSFORMER = "keyValueTransformer";
        public static final String TYPE_MERGER = "merger";
        public static final String TYPE_METADATATRANSFORMER = "metadataTransformer";
        public static final String TYPE_PREDICATE = "predicate";
        public static final String TYPE_REDUCER = "reducer";
        public static final String TYPE_STREAMPARTITIONER = "streamPartitioner";
        public static final String TYPE_TIMESTAMPEXTRACTOR = "timestampExtractor";
        public static final String TYPE_TOPICNAMEEXTRACTOR = "topicNameExtractor";
        public static final String TYPE_VALUEJOINER = "valueJoiner";
        public static final String TYPE_VALUETRANSFORMER = "valueTransformer";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Parameters {
            public static final String NAME = "name";
            public static final String TYPE = "type";
            public static final String DEFAULT_VALUE = "defaultValue";
        }
    }

    public static final String PRODUCERS = "producers";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Producers {
        public static final String GENERATOR = "generator";
        public static final String INTERVAL = "interval";
        public static final String CONDITION = "condition";
        public static final String TARGET = "to";
        public static final String COUNT = "count";
        public static final String UNTIL = "until";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Operations {
        public static final String NAME_ATTRIBUTE = "name";
        public static final String TYPE_ATTRIBUTE = "type";
        public static final String STORE_ATTRIBUTE = "store";
        public static final String STORE_NAMES_ATTRIBUTE = "stores";

        public static final String AGGREGATE = "aggregate";
        public static final String COGROUP = "cogroup";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Aggregate {
            public static final String INITIALIZER = "initializer";
            public static final String AGGREGATOR = "aggregator";
            public static final String MERGER = "merger";
            public static final String ADDER = "adder";
            public static final String SUBTRACTOR = "subtractor";
        }

        public static final String AS = "as";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class As {
        }

        public static final String BRANCH = "branch";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Branch {
            public static final String PREDICATE = "if";
        }

        public static final String CONVERT_KEY = "convertKey";
        public static final String CONVERT_KEY_VALUE = "convertKeyValue";
        public static final String CONVERT_VALUE = "convertValue";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Convert {
            public static final String INTO = "into";
        }

        public static final String COUNT = "count";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Count {
        }

        public static final String FILTER = "filter";
        public static final String FILTER_NOT = "filterNot";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Filter {
            public static final String PREDICATE = "if";
        }

        public static final String FOR_EACH = "forEach";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class ForEach {
        }

        public static final String GROUP_BY = "groupBy";
        public static final String GROUP_BY_KEY = "groupByKey";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class GroupBy {
            public static final String MAPPER = "mapper";
        }

        public static final String JOIN = "join";
        public static final String LEFT_JOIN = "leftJoin";
        public static final String OUTER_JOIN = "outerJoin";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Join {
            public static final String FOREIGN_KEY_EXTRACTOR = "foreignKeyExtractor";
            public static final String VALUE_JOINER = "valueJoiner";
            public static final String MAPPER = "mapper";
            public static final String TIME_DIFFERENCE = "timeDifference";
            public static final String GRACE = "grace";
            public static final String PARTITIONER = "partitioner";
            public static final String OTHER_PARTITIONER = "otherPartitioner";

            public static final String WITH_STREAM = "stream";
            public static final String WITH_TABLE = "table";
            public static final String WITH_GLOBAL_TABLE = "globalTable";
        }

        public static final String MERGE = "merge";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Merge {
            public static final String STREAM = "stream";
        }

        public static final String PEEK = "peek";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Peek {
            public static final String FOR_EACH = "forEach";
        }

        public static final String PRINT = "print";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Print {
            public static final String FILENAME = "filename";
            public static final String LABEL = "label";
            public static final String MAPPER = "mapper";
        }

        public static final String REDUCE = "reduce";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Reduce {
            public static final String REDUCER = "reducer";
            public static final String ADDER = "adder";
            public static final String SUBTRACTOR = "subtractor";
        }

        public static final String REPARTITION = "repartition";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Repartition {
            public static final String NUMBER_OF_PARTITIONS = "numberOfPartitions";
            public static final String PARTITIONER = "partitioner";
        }

        public static final String SUPPRESS = "suppress";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Suppress {
            public static final String UNTIL = "until";
            public static final String UNTIL_TIME_LIMIT = "timeLimit";
            public static final String UNTIL_WINDOW_CLOSES = "windowCloses";
            public static final String DURATION = "duration";
            public static final String BUFFER_MAXBYTES = "maxBytes";
            public static final String BUFFER_MAXRECORDS = "maxRecords";
            public static final String BUFFER_FULL_STRATEGY = "bufferFullStrategy";
            public static final String BUFFER_FULL_STRATEGY_EMIT = "emitEarlyWhenFull";
            public static final String BUFFER_FULL_STRATEGY_SHUTDOWN = "shutdownWhenFull";
        }

        public static final String TO_TOPIC = "to";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class To {
            public static final String PARTITIONER = "partitioner";
        }

        public static final String TO_TOPIC_NAME_EXTRACTOR = "toTopicNameExtractor";

        public static class ToTopicNameExtractor {
            public static final String TOPIC_NAME_EXTRACTOR = "topicNameExtractor";
            public static final String PARTITIONER = "partitioner";
        }

        public static final String TO_STREAM = "toStream";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class ToStream {
            public static final String MAPPER = "mapper";
        }

        public static final String TO_TABLE = "toTable";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class ToTable {
        }

        public static final String FLATMAP = "flatMap";
        public static final String FLATMAP_VALUES = "flatMapValues";
        public static final String MAP_KEY = "mapKey";
        public static final String SELECT_KEY = "selectKey";
        public static final String MAP = "map";
        public static final String MAP_KEY_VALUE = "mapKeyValue";
        public static final String MAP_VALUE = "mapValue";
        public static final String MAP_VALUES = "mapValues";

        public static final String TRANSFORM_KEY = "transformKey";
        public static final String TRANSFORM_KEY_VALUE = "transformKeyValue";
        public static final String TRANSFORM_KEY_VALUE_TO_KEY_VALUE_LIST = "transformKeyValueToKeyValueList";
        public static final String TRANSFORM_KEY_VALUE_TO_VALUE_LIST = "transformKeyValueToValueList";
        public static final String TRANSFORM_METADATA = "transformMetadata";
        public static final String TRANSFORM_VALUE = "transformValue";

        @NoArgsConstructor(access = AccessLevel.PRIVATE)
        public static class Transform {
            public static final String MAPPER = "mapper";
        }

        public static final String WINDOW_BY_TIME = "windowByTime";
        public static final String WINDOW_BY_SESSION = "windowBySession";
    }

    public static final String PIPELINES = "pipelines";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Pipelines {
        public static final String NAME = "name";
        public static final String FROM = "from";
        public static final String VIA = "via";
    }

    public static final String STORES = "stores";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Stores {
        public static final String NAME = "name";
        public static final String TYPE = "type";
        public static final String PERSISTENT = "persistent";
        public static final String TIMESTAMPED = "timestamped";
        public static final String VERSIONED = "versioned";
        public static final String HISTORY_RETENTION = "historyRetention";
        public static final String SEGMENT_INTERVAL = "segmentInterval";
        public static final String KEY_TYPE = "keyType";
        public static final String VALUE_TYPE = "valueType";
        public static final String RETENTION = "retention";
        public static final String CACHING = "caching";
        public static final String LOGGING = "logging";
        public static final String TYPE_KEY_VALUE = "keyValue";
        public static final String TYPE_SESSION = "session";
        public static final String TYPE_WINDOW = "window";
        public static final String WINDOW_SIZE = "windowSize";
        public static final String RETAIN_DUPLICATES = "retainDuplicates";
    }

    public static final String STREAMS = "streams";
    public static final String TABLES = "tables";
    public static final String GLOBAL_TABLES = "globalTables";

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Streams {
        public static final String STORE = "store";
        public static final String TOPIC = "topic";
        public static final String KEY_TYPE = "keyType";
        public static final String VALUE_TYPE = "valueType";
        public static final String TIMESTAMP_EXTRACTOR = "timestampExtractor";
        public static final String OFFSET_RESET_POLICY = "offsetResetPolicy";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class SessionWindows {
        public static final String INACTIVITY_GAP = "inactivityGap";
        public static final String GRACE = "grace";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TimeWindows {
        public static final String WINDOW_TYPE = "windowType";
        public static final String TYPE_TUMBLING = "tumbling";
        public static final String TYPE_HOPPING = "hopping";
        public static final String TYPE_SLIDING = "sliding";
        public static final String TIME_DIFFERENCE = "timeDifference";
        public static final String DURATION = "duration";
        public static final String ADVANCE_BY = "advanceBy";
        public static final String GRACE = "grace";
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Types {
        // Type names used in the exported JSON Schema
        public static final String FUNCTION_TYPE = "FunctionType";
        public static final String WITH_PREFIX = "With";
        public static final String WITH_IMPLICIT_TYPE_POSTFIX = WITH_PREFIX + "ImplicitType";
        public static final String WITH_STREAM = WITH_PREFIX + "Stream";
        public static final String WITH_TABLE = WITH_PREFIX + "Table";
        public static final String WITH_GLOBAL_TABLE = WITH_PREFIX + "GlobalTable";
    }
}
