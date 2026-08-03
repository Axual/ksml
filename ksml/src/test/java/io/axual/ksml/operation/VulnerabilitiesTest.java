package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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
import io.axual.ksml.testutil.KSMLTopologyTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for various found KSML shell escapes.
 * This test tries to use the exploit to create a shell with a curl to localhost:5555 and starts a separate thread listening on port 5555,
 * which will increase a counter every time a request is received.
 * Verification is a check to see if an exception is thrown when the pipeline is run.
 */
@ExtendWith(KSMLTestExtension.class)
@Slf4j
@Disabled("With a correct Python environment, this test should throw an exception.")
@SuppressWarnings("java:S2187")
public class VulnerabilitiesTest {

    @KSMLTopic(topic = "ksml_sensordata_avro", valueSerde = KSMLTopic.SerdeType.AVRO)
    TestInputTopic<String, GenericRecord> sensorIn;

    @KSMLTopic(topic = "ksml_sensordata_avro")
    protected TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "ksml_sensordata_copy")
    protected TestOutputTopic<String, String> outputTopic;

    @KSMLDriver
    TopologyTestDriver topologyTestDriver;

    static ExecutorService executorService = Executors.newSingleThreadExecutor();

    static AtomicInteger counter = new AtomicInteger(0);

    @BeforeAll
    static void startServer() {
        executorService.execute(() -> listener(5555));
    }

    @AfterAll
    static void cleanupServer() {
        executorService.shutdown();
    }

    /**
     * Test that attacks via HashMap.getClass() are blocked by the sandbox.
     * This attack is blocked because getClass() is not accessible on the polyglot type object.
     */
     @KSMLTest(topology = "pipelines/vulnerable-hashmap.yaml")
     void testVulnerableHashMap() {
         int oldcounter = counter.get();
         // The sandbox blocks getClass() access, so this should throw an exception
         assertThatThrownBy(() -> inputTopic.pipeInput("key1", "value1"))
                 .as("Sandbox should block getClass() access on HashMap")
                 .isInstanceOf(Exception.class);
         assertThat(counter.get()).as("No curl request should be received").isEqualTo(oldcounter);
     }

    /**
     * Test that attacks via log, loggerbridge, and metrics objects are blocked.
     * These attacks fail silently (globalCode runs before objects are initialized).
     */
    @KSMLTopologyTest(topologies = {"pipelines/vulnerable-log.yaml","pipelines/vulnerable-loggerbridge.yaml","pipelines/vulnerable-metrics.yaml"})
    @DisplayName("check vulnerabilities in log and metrics")
    void testVulnerableLogAndMetrics() {
        assertThatThrownBy(() -> {
            int oldcounter = counter.get();
            inputTopic.pipeInput("key1", "value1");
            assertThat(outputTopic.isEmpty()).as("record should be copied").isFalse();
            var keyValue = outputTopic.readKeyValue();
            log.info("Output topic key={}, value={}", keyValue.key, keyValue.value);
            assertThat(counter.get()).as("No curl request should be received").isEqualTo(oldcounter);
        })
                .as("trying to exploit log and metrics should result in RuntimeException")
                .isInstanceOf(RuntimeException.class)
                .cause()
                .as("`getClass' should be blocked by the sandbox.")
                .hasMessageContaining("foreign object has no attribute 'getClass'");
    }

    @KSMLTest(topology = "pipelines/vulnerable-state-store.yaml", schemaDirectory = "schemas")
    @DisplayName("check vulnerabilities in state stores")
    void testVulnerableStateStore() {
        assertThatThrownBy(() -> {
            int oldCounter = counter.get();

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
                assertThat(sensor1Data.get("city")).isEqualTo(new DataString("Amsterdam"));
                assertThat(sensor1Data.get("value")).isEqualTo(new DataString("70"));

                // and the counter should have been incremented
                assertThat(counter.get()).as("No curl request should be received").isEqualTo(oldCounter);
        })
                .as("Trying to exploit state stores should result on RuntimeException")
                .isInstanceOf(RuntimeException.class)
                .cause()
                .as("`getClass' should be blocked by the sandbox.")
                .hasMessageContaining("foreign object has no attribute 'getClass'");
    }

    @KSMLTest(topology = "pipelines/vulnerable-versioned-state-store.yaml", schemaDirectory = "schemas")
    @DisplayName("check vulnerabilities in versioned state stores")
    void testVulnerableVersionedStateStore() {
        assertThatThrownBy(() -> {
            int oldCounter = counter.get();

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
                assertThat(sensor1Data.get("city")).isEqualTo(new DataString("Amsterdam"));
                assertThat(sensor1Data.get("value")).isEqualTo(new DataString("70"));

                // and the counter should have been incremented
                assertThat(counter.get()).as("No curl request should be received").isEqualTo(oldCounter);
        })
                .as("Trying to exploit state stores should result on RuntimeException")
                .isInstanceOf(RuntimeException.class)
                .cause()
                .as("`getClass' should be blocked by the sandbox.")
                .hasMessageContaining("foreign object has no attribute 'getClass'");
    }

    /**
     * Try to set up a port listener that flags when it gets a connection, so we don't need an external terminal and netcat.
     * @param portNumber the port number to listen on.
     */
    static void listener(int portNumber) {
        log.info("set up test server on port {}", portNumber);
        try (ServerSocket serverSocket = new ServerSocket(portNumber)) {
            log.info("Server socket opened on port {}", portNumber);
            while (true) {
                var clientSocket = serverSocket.accept();
                log.info("===============> received a request");
                counter.incrementAndGet();
                log.info("Received request from {}", clientSocket.getInetAddress());
                handleClient(clientSocket);
            }
        } catch (IOException e) {
            log.error("Error setting up server socket", e);
        }
    }

    /**
     * Handle an incoming curl request by sending an empty response.
     * @param clientSocket the client socket.
     */
    static void handleClient(Socket clientSocket) {
        log.info("=====> handle client request");
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))
            ) {
            // read the whole request
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isEmpty()) {
                    break;
                }
            }
            // send an empty response
            out.write("HTTP/1.1 200 OK\r\n\r\n");
        } catch (IOException e) {
            log.error("Error handling request", e);
        }
    }
}
