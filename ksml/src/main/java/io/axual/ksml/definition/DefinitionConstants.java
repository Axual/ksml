package io.axual.ksml.definition;

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


import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;

public class DefinitionConstants {
    public static final String PARAM_AGGREGATED_VALUE = "aggregatedValue";
    public static final String PARAM_NUM_PARTITIONS = "numPartitions";
    public static final String PARAM_RECORD_CONTEXT = "recordContext";
    public static final String PARAM_TOPIC = "topic";
    public static final String PARAM_KEY = "key";
    public static final String PARAM_VALUE = "value";
    public static final String PARAM_VALUE1 = "value1";
    public static final String PARAM_VALUE2 = "value2";
    public static final String PARAM_HEADERS = "headers";

    private DefinitionConstants() {
    }

    public static final ParameterDefinition[] NO_PARAMETERS = new ParameterDefinition[]{};
    public static final ParameterDefinition[] FOREIGN_KEY_EXTRACTOR_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_VALUE, DataType.UNKNOWN)};
    public static final ParameterDefinition[] KEY_AND_TWO_VALUE_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_KEY, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE1, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE2, DataType.UNKNOWN)};
    public static final ParameterDefinition[] KEY_VALUE_AGGREGATEDVALUE_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_KEY, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_AGGREGATED_VALUE, DataType.UNKNOWN)};
    public static final ParameterDefinition[] KEY_VALUE_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_KEY, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE, DataType.UNKNOWN)};
    public static final DataType HEADERS_TYPE = new ListType(new ListType(DataString.DATATYPE));
    public static final ParameterDefinition[] KEY_VALUE_HEADERS_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_KEY, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_HEADERS, HEADERS_TYPE)};
    public static final ParameterDefinition[] STREAM_PARTITIONER_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_TOPIC, DataString.DATATYPE),
            new ParameterDefinition(PARAM_KEY, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_NUM_PARTITIONS, DataInteger.DATATYPE)};
    public static final ParameterDefinition[] TOPIC_NAME_EXTRACTOR_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_KEY, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_RECORD_CONTEXT, new MapType(DataType.UNKNOWN))};
    public static final ParameterDefinition[] TWO_VALUE_PARAMETERS = new ParameterDefinition[]{
            new ParameterDefinition(PARAM_VALUE1, DataType.UNKNOWN),
            new ParameterDefinition(PARAM_VALUE2, DataType.UNKNOWN)};
}
