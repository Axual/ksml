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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static io.axual.ksml.testutil.JsonVerifier.verifyJson;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLJoinTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> sensorIn;

    @KSMLTopic(topic = "ksml_sensoralert_settings", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> alertSettings;

    @KSMLTopic(topic = "ksml_sensoralert")
    TestOutputTopic<String, String> sensorAlerts;

    @KSMLTopic(topic = "left_topic")
    TestInputTopic<String, String> leftTable;

    @KSMLTopic(topic = "right_topic")
    TestInputTopic<String, String> rightTable;

    @KSMLTopic(topic = "output_topic")
    TestOutputTopic<String, String> tableJoinOut;

    @KSMLTopic(topic = "count_topic", valueSerde = KSMLTopic.SerdeType.LONG)
    TestOutputTopic<String, Long> joinCountTopic;

    @KSMLTest(topology = "pipelines/test-join-table-tombstone.yaml")
    void testTableJoinTombstoneDeletesRow() {
        leftTable.pipeInput("key1", "{\"v\": 1}");
        rightTable.pipeInput("key1", "{\"v\": 2}");
        leftTable.pipeInput("key1", (String) null);

        // the delete on the left table should delete the joined row, like in Kafka Streams
        final var result = tableJoinOut.readKeyValuesToList();
        assertThat(result).hasSize(2);
        assertThat(MAPPER.readTree(result.get(0).value)).isEqualTo(MAPPER.readTree("{\"left\": 1, \"right\": 2}"));
        assertThat(result.get(1)).isEqualTo(new KeyValue<>("key1", null));
    }

    @KSMLTest(topology = "pipelines/test-joining.yaml", schemaDirectory = "schemas")
    void testJoin() throws Exception {

        // given that we trigger on humidity > 50 in Amsterdam and temp > 25 in Utrecht
        SensorAlertSetting.AlertSetting humidityAlert = SensorAlertSetting.AlertSetting.builder()
                .type(SensorAlertSetting.AlertSetting.Type.HUMIDITY)
                .unit("%")
                .alertAbove("50").build();
        SensorAlertSetting.AlertSetting temperatureAlert = SensorAlertSetting.AlertSetting.builder()
                .type(SensorAlertSetting.AlertSetting.Type.TEMPERATURE)
                .unit("C")
                .alertAbove("25").build();
        alertSettings.pipeInput("Amsterdam", SensorAlertSetting.builder().alertSetting(humidityAlert).build().toRecord());
        alertSettings.pipeInput("Utrecht", SensorAlertSetting.builder().alertSetting(temperatureAlert).build().toRecord());

        // and we get events with a higher reading in matching cities
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

        // we should see alerts containing the alert that was triggered, joined with the sensor data that triggered it
        assertThat(sensorAlerts.getQueueSize()).isEqualTo(2);
        List<String> valuesList = sensorAlerts.readValuesToList();
        String alert1 = valuesList.getFirst();
        log.info("alert1 = {}", alert1);
        verifyJson(alert1)
                .hasNode("alert").withChild("type").withTextValue("HUMIDITY")
                .hasNode("alert").withChild("alertAbove").withTextValue("50")
                .hasNode("sensordata").withChild("city").withTextValue("Amsterdam")
                .hasNode("sensordata").withChild("type").withTextValue("HUMIDITY")
                .hasNode("sensordata").withChild("value").withTextValue("80");
    }

    @KSMLTest(topology = "pipelines/test-join-joiner-none.yaml")
    void testValueJoinerNoneIsTombstone() {
        rightTable.pipeInput("a", "{\"name\": \"Ann\"}");
        leftTable.pipeInput("a", "{\"amount\": 1}");
        leftTable.pipeInput("a", "{\"amount\": 0}");
        leftTable.pipeInput("a", "{\"amount\": 2}");

        // the joiner returns None for an amount of 0, which is a tombstone, so the count skips it
        assertThat(joinCountTopic.readKeyValuesToList()).containsExactly(
                new KeyValue<>("a", 1L),
                new KeyValue<>("a", 2L));
    }
}
