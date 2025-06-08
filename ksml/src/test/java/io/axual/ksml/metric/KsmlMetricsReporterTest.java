package io.axual.ksml.metric;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.kafka.streams.processor.api.Processor;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.DoubleSupplier;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import lombok.extern.slf4j.Slf4j;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class KsmlMetricsReporterIntegrationTest {

    private static final String JMX_DOMAIN = "ksml";
    private static final String SOURCE_TOPIC = "input-topic";
    private static final String SINK_TOPIC = "output-topic";

    private MetricsRegistry metricsRegistry;
    private MBeanServer mBeanServer;
    private JmxReporter jmxReporter;
    private KsmlMetricsReporter reporter;

    private final List<TopologyTestDriver> drivers = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        MetricRegistry underlying = new MetricRegistry();
        metricsRegistry = new MetricsRegistry(underlying);
        mBeanServer = ManagementFactory.getPlatformMBeanServer();

        jmxReporter = JmxReporter.forRegistry(underlying)
                .inDomain(JMX_DOMAIN)
                .createsObjectNamesWith(new MetricObjectNameFactory(List.of()))
                .build();
        jmxReporter.start();
    }

    @AfterEach
    void tearDown() {
        drivers.forEach(driver -> {
            try {
                driver.close();
            } catch (Exception e) {
                log.warn("Failed to close TopologyTestDriver", e);
            }
        });

        if (jmxReporter != null) {
            try {
                jmxReporter.stop();
                jmxReporter.close();
            } catch (Exception e) {
                log.warn("Failed to close JMX reporter", e);
            }
        }
        if (reporter != null) {
            try {
                reporter.close();
            } catch (Exception e) {
                log.warn("Failed to close metrics reporter", e);
            }
        }
    }

    @Test
    void shouldRegisterExpectedMBeanForSingleMetric() throws Exception {
        String nodeId = "aggregate_pipelines_main_via_step5_peek";
        TopologyDescription td = buildTopologyDescription(nodeId);

        reporter = new KsmlMetricsReporter(KsmlTagEnricher.from(td));
        reporter.setRegistry(metricsRegistry);

        MutableDoubleSupplier value = new MutableDoubleSupplier(302.0);
        KafkaMetric metric = kafkaMetric("min", nodeId, value);

        reporter.metricChange(metric);

        ObjectName expectedName = new ObjectName(
                JMX_DOMAIN +
                        ":type=record_e2e_latency_min_ms," +
                        "namespace=aggregate,operation_name=peek,pipeline=main," +
                        "processor_node_id=" + nodeId + "," +
                        "step=5,unit=ms");

        assertThat(mBeanServer.isRegistered(expectedName)).isTrue();
        assertThat(mBeanServer.getAttribute(expectedName, "Value"))
                .isEqualTo(302.0);
    }


    @Test
    void shouldRegisterMultipleBeans() throws Exception {
        TopologyDescription topologyDescription = buildTopologyDescription(
                "ns1_pipelines_pipe1_source",
                "ns1_pipelines_pipe1_via_step1_filter",
                "ns2_pipelines_pipe2_sink");

        reporter = new KsmlMetricsReporter(KsmlTagEnricher.from(topologyDescription));
        reporter.setRegistry(metricsRegistry);

        record TestCase(String nodeId, String latType, double val,
                        String ns, String pipe, String op, String step) {
        }

        List<TestCase> cases = List.of(
                new TestCase("ns1_pipelines_pipe1_source", "avg", 100.0, "ns1", "pipe1", "source", null),
                new TestCase("ns1_pipelines_pipe1_via_step1_filter", "max", 200.0, "ns1", "pipe1", "filter", "1"),
                new TestCase("ns2_pipelines_pipe2_sink", "min", 50.0, "ns2", "pipe2", "sink", null)
        );

        cases.forEach(testCase -> {
            MutableDoubleSupplier val = new MutableDoubleSupplier(testCase.val());
            reporter.metricChange(kafkaMetric(testCase.latType(), testCase.nodeId(), val));
        });

        for (TestCase testCase : cases) {
            String objectName =
                    "%s:type=record_e2e_latency_%s_ms,namespace=%s,operation_name=%s,"
                            + "pipeline=%s,processor_node_id=%s%s,unit=ms";
            ObjectName name = new ObjectName(objectName.formatted(
                    JMX_DOMAIN, testCase.latType(), testCase.ns(), testCase.op(),
                    testCase.pipe(), testCase.nodeId(),
                    testCase.step() != null ? ",step=" + testCase.step() : ""));

            assertThat(mBeanServer.isRegistered(name)).isTrue();
            assertThat(mBeanServer.getAttribute(name, "Value"))
                    .isEqualTo(testCase.val());
        }
    }

    @Test
    void shouldReflectUpdatedValue() throws Exception {
        String nodeId = "test_pipelines_main_forEach";
        TopologyDescription td = buildTopologyDescription(nodeId);

        reporter = new KsmlMetricsReporter(KsmlTagEnricher.from(td));
        reporter.setRegistry(metricsRegistry);

        MutableDoubleSupplier value = new MutableDoubleSupplier(100.0);
        KafkaMetric metric = kafkaMetric("avg", nodeId, value);

        reporter.metricChange(metric);

        ObjectName on = new ObjectName(
                JMX_DOMAIN + ":type=record_e2e_latency_avg_ms,namespace=test,operation_name=forEach,"
                        + "pipeline=main,processor_node_id=" + nodeId + ",unit=ms");

        assertThat(mBeanServer.getAttribute(on, "Value")).isEqualTo(100.0);

        value.set(200.0);

        assertThat(mBeanServer.getAttribute(on, "Value")).isEqualTo(200.0);
    }


    @Test
    void shouldUnregisterBeanOnRemoval() throws Exception {
        String nodeId = "test_pipelines_main_forEach";
        TopologyDescription topologyDescription = buildTopologyDescription(nodeId);

        reporter = new KsmlMetricsReporter(KsmlTagEnricher.from(topologyDescription));
        reporter.setRegistry(metricsRegistry);

        MutableDoubleSupplier value = new MutableDoubleSupplier(100.0);
        KafkaMetric metric = kafkaMetric("avg", nodeId, value);

        reporter.metricChange(metric);

        ObjectName objectName = new ObjectName(
                JMX_DOMAIN + ":type=record_e2e_latency_avg_ms,namespace=test,operation_name=forEach,"
                        + "pipeline=main,processor_node_id=" + nodeId + ",unit=ms");

        assertThat(mBeanServer.isRegistered(objectName)).isTrue();

        reporter.metricRemoval(metric);

        assertThat(mBeanServer.isRegistered(objectName)).isFalse();
    }


    @Test
    void shouldIgnoreNonLatencyMetrics() throws Exception {
        String nodeId = "test_pipelines_main_forEach";
        TopologyDescription td = buildTopologyDescription(nodeId);

        reporter = new KsmlMetricsReporter(KsmlTagEnricher.from(td));
        reporter.setRegistry(metricsRegistry);

        MutableDoubleSupplier supplier = new MutableDoubleSupplier(1_000.0);

        MetricName metricName = new MetricName(
                "process-rate",
                "stream-processor-node-metrics",
                "",
                Map.of("processor-node-id", nodeId));

        Measurable measurable = (cfg, now) -> supplier.getAsDouble();

        KafkaMetric nonLatencyKafkaMetric = new KafkaMetric(
                new Object(), metricName, measurable, new MetricConfig(), Time.SYSTEM);

        reporter.metricChange(nonLatencyKafkaMetric);

        assertThat(mBeanServer.queryNames(new ObjectName(JMX_DOMAIN + ":*"), null))
                .isEmpty();
    }


    @Test
    void closeShouldRemoveAllBeans() throws Exception {
        TopologyDescription topologyDescription = buildTopologyDescription(
                "test_pipelines_main_forEach",
                "test_pipelines_main_map");

        reporter = new KsmlMetricsReporter(KsmlTagEnricher.from(topologyDescription));
        reporter.setRegistry(metricsRegistry);

        reporter.metricChange(kafkaMetric(
                "avg", "test_pipelines_main_forEach", new MutableDoubleSupplier(100.0)));
        reporter.metricChange(kafkaMetric(
                "max", "test_pipelines_main_map", new MutableDoubleSupplier(200.0)));

        assertThat(mBeanServer.queryNames(new ObjectName(JMX_DOMAIN + ":*"), null))
                .hasSize(2);

        reporter.close();

        assertThat(mBeanServer.queryNames(new ObjectName(JMX_DOMAIN + ":*"), null))
                .isEmpty();
    }

    /**
     * Builds a minimal {@link Topology} whose processor-node IDs match the supplied names,
     * runs it through {@link TopologyTestDriver} (so we know it’s valid), and returns the
     * resulting {@link TopologyDescription}.
     */

    private TopologyDescription buildTopologyDescription(String... processorNodeIds) {
        Topology topology = new Topology();
        topology.addSource("source", SOURCE_TOPIC);

        for (String nodeId : processorNodeIds) {
            topology.addProcessor(
                    nodeId,
                    () -> new Processor<Object, Object, Void, Void>() {
                        @Override
                        public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context) {
                            // No-op init
                        }

                        @Override
                        public void process(org.apache.kafka.streams.processor.api.Record<Object, Object> kafkaRecord) {
                            // No-op for test purposes
                        }

                        @Override
                        public void close() {
                            // No-op close
                        }
                    },
                    "source"
            );
        }

        topology.addSink("sink", SINK_TOPIC,
                processorNodeIds.length == 0 ? "source"
                        : processorNodeIds[processorNodeIds.length - 1]);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        drivers.add(new TopologyTestDriver(topology, props));

        return topology.describe();
    }


    private static final class MutableDoubleSupplier implements DoubleSupplier {
        private volatile double value;

        MutableDoubleSupplier(double initial) {
            this.value = initial;
        }

        void set(double v) {
            this.value = v;
        }

        @Override
        public double getAsDouble() {
            return value;
        }
    }

    /**
     * Builds a genuine {@link KafkaMetric} that reports the latency value supplied
     * by {@code valueSupplier}.  Because we keep a reference to that supplier, the
     * test can change the metric’s value later simply by calling
     * {@code supplier.set(newValue)} – no Mockito stubbing required.
     */
    private static KafkaMetric kafkaMetric(String latencySuffix,
                                           String nodeId,
                                           MutableDoubleSupplier valueSupplier) {

        MetricName metricName = new MetricName(
                "record-e2e-latency-" + latencySuffix,
                "stream-processor-node-metrics",
                "",
                Map.of("processor-node-id", nodeId)
        );

        Measurable measurable = (cfg, now) -> valueSupplier.getAsDouble();

        return new KafkaMetric(
                new Object(),
                metricName,
                measurable,
                new MetricConfig(),
                Time.SYSTEM
        );
    }
}
