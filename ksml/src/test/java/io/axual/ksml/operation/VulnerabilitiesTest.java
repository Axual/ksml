package io.axual.ksml.operation;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import io.axual.ksml.testutil.KSMLTopologyTest;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THIS IS NOT A STANDARD UNIT TEST.
 * For verification, open netcat in a separate terminal and set it to listen on port 5555. If the vulnerability exists, you will
 * see an incoming GET request from KSML.
 */
@ExtendWith(KSMLTestExtension.class)
@Slf4j
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

    @KSMLTopologyTest(topologies = {"pipelines/vulnerable-log.yaml","pipelines/vulnerable-loggerbridge.yaml","pipelines/vulnerable-metrics.yaml"})
    void testVulnerableLogAndMetrics() {
        int oldcounter = counter.get();
        inputTopic.pipeInput("key1", "value1");
        assertFalse(outputTopic.isEmpty(), "record should be copied");
        var keyValue = outputTopic.readKeyValue();
        System.out.printf("Output topic key=%s, value=%s%n", keyValue.key, keyValue.value);
        assertEquals(oldcounter + 1, counter.get(), "One or more curl requests received");
    }

    @KSMLTest(topology = "pipelines/vulnerable-state-store.yaml", schemaDirectory = "schemas")
    void testVulnerableJoin() {

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
        assertEquals(new DataString("Amsterdam"), sensor1Data.get("city"));
        assertEquals(new DataString("70"), sensor1Data.get("value"));

        // and the counter should have been incremented
        assertTrue(counter.get() > oldCounter, "One or more curl requests received");
    }

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
