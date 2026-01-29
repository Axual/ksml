package io.axual.ksml.operation;

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
import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KSMLTestExtension.class)
public class KSMLStateStoreTest {

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> sensorIn;

    @KSMLDriver
    TopologyTestDriver topologyTestDriver;

    @KSMLTest(topology = "pipelines/test-state-store.yaml", schemaDirectory = "schemas")
    @DisplayName("Test messages are stored in a key/value store")
    void testJoin() {

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

    @KSMLTest(topology = "pipelines/test-state-store-timestamped.yaml", schemaDirectory = "schemas")
    @DisplayName("A timestamped key/value store works")
    void testJoinTimestamped() {

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

    @KSMLTest(topology = "pipelines/test-state-store-timestamped-factory.yaml", schemaDirectory = "schemas")
    @DisplayName("ValueAndTimestamp can be made with a factory method")
    void testJoinTimestampedFactory() {

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
