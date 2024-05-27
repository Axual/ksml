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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static io.axual.ksml.testutil.JsonVerifier.verifyJson;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KSMLTestExtension.class)
public class KSMLJoinTest {

    TestInputTopic sensorIn;

    TestInputTopic alertSettings;

    TestOutputTopic sensorAlerts;

    @KSMLTest(topology = "pipelines/test-joining.yaml", schemapath = "pipelines",
        inputTopics = {@KSMLTopic(topic = "ksml_sensordata_avro", variable = "sensorIn", valueSerde = KSMLTopic.SerdeType.AVRO),
                        @KSMLTopic(topic = "ksml_sensoralert_settings", variable = "alertSettings", valueSerde = KSMLTopic.SerdeType.AVRO)},
        outputTopics = {@KSMLTopic(topic = "ksml_sensoralert", variable = "sensorAlerts")})
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
        assertEquals(2, sensorAlerts.getQueueSize());
        List valuesList = sensorAlerts.readValuesToList();
        String alert1 = valuesList.getFirst().toString();
        System.out.println("alert1 = " + alert1);
        verifyJson(alert1)
                .hasNode("alert").withChild("type").withTextValue("HUMIDITY")
                .hasNode("alert").withChild("alertAbove").withTextValue("50")
                .hasNode("sensordata").withChild("city").withTextValue("Amsterdam")
                .hasNode("sensordata").withChild("type").withTextValue("HUMIDITY")
                .hasNode("sensordata").withChild("value").withTextValue("80");
    }
}
