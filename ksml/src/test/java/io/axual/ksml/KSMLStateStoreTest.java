package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KSMLTestExtension.class)
public class KSMLStateStoreTest {

    TestInputTopic sensorIn;

    TopologyTestDriver topologyTestDriver;

    @KSMLTest(topology = "pipelines/test-state-store.yaml", schemapath = "pipelines",
            inputTopics = {@KSMLTopic(topic = "ksml_sensordata_avro", variable = "sensorIn", valueSerde = KSMLTopic.SerdeType.AVRO)}
            , outputTopics = {}, testDriverRef = "topologyTestDriver")
    void testJoin() throws Exception {

        // given that we get events with a higher reading in matching cities
        sensorIn.pipeInput("sensor1", SensorData.builder()
                .city("Amsterdam")
                .type(SensorData.SensorType.HUMIDITY)
                .unit("%")
                .value("80")
                .build().toRecord());
        sensorIn.pipeInput("sensor2", SensorData.builder()
                .city("Utrecht")
                .type(SensorData.SensorType.TEMPERATURE)
                .unit("C")
                .value("26")
                .build().toRecord());

        // and a new value for sensor1
        sensorIn.pipeInput("sensor1", SensorData.builder()
                .city("Amsterdam")
                .type(SensorData.SensorType.HUMIDITY)
                .unit("%")
                .value("70")
                .build().toRecord());

        // the last value for sensor1 is present in the store named "last_sensor_data_store"
        KeyValueStore<Object, Object> lastSensorDataStore = topologyTestDriver.getKeyValueStore("last_sensor_data_store");
        DataStruct sensor1Data = (DataStruct) lastSensorDataStore.get("sensor1");
        assertEquals(new DataString("Amsterdam"), sensor1Data.get("city"));
        assertEquals(new DataString("70"), sensor1Data.get("value"));
    }
}
