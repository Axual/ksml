package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import io.axual.ksml.TopologyGenerator;
import io.axual.ksml.client.generic.ResolvingClientConfig;
import io.axual.ksml.client.producer.ResolvingProducerConfig;
import io.axual.ksml.client.serde.ResolvingDeserializer;
import io.axual.ksml.client.serde.ResolvingSerializer;
import io.axual.ksml.data.schema.SchemaLibrary;
import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.execution.ExecutionErrorHandler;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.notation.avro.AvroNotation;
import io.axual.ksml.notation.avro.AvroSchemaLoader;
import io.axual.ksml.notation.binary.BinaryNotation;
import io.axual.ksml.notation.csv.CsvDataObjectConverter;
import io.axual.ksml.notation.csv.CsvNotation;
import io.axual.ksml.notation.csv.CsvSchemaLoader;
import io.axual.ksml.notation.json.JsonDataObjectConverter;
import io.axual.ksml.notation.json.JsonNotation;
import io.axual.ksml.notation.json.JsonSchemaLoader;
import io.axual.ksml.notation.soap.SOAPDataObjectConverter;
import io.axual.ksml.notation.soap.SOAPNotation;
import io.axual.ksml.notation.xml.XmlDataObjectConverter;
import io.axual.ksml.notation.xml.XmlNotation;
import io.axual.ksml.notation.xml.XmlSchemaLoader;
import io.axual.ksml.rest.server.StreamsQuerier;
import io.axual.ksml.runner.config.KSMLConfig;
import io.axual.ksml.runner.config.KSMLErrorHandlingConfig;
import io.axual.ksml.runner.streams.KSMLClientSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.TopologyConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaBackend implements Backend {
    private final KafkaStreams kafkaStreams;
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);

    public KafkaBackend(KSMLConfig ksmlConfig, Map<String, String> kafkaConfig) {
        log.info("Constructing Kafka Backend");

        HashMap<String, Object> streamsConfig = kafkaConfig != null ? new HashMap<>(kafkaConfig) : new HashMap<>();
        // Explicit configs can overwrite those from the map
        streamsConfig.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        streamsConfig.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        streamsConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, ExecutionErrorHandler.class);
        if (streamsConfig.containsKey(ResolvingClientConfig.GROUP_ID_PATTERN_CONFIG)
                || streamsConfig.containsKey(ResolvingClientConfig.TOPIC_PATTERN_CONFIG)
                || streamsConfig.containsKey(ResolvingProducerConfig.TRANSACTIONAL_ID_PATTERN_CONFIG)) {
            log.info("Using resolving clients for Kafka");
            streamsConfig.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, KSMLClientSupplier.class.getCanonicalName());
        }

        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, ksmlConfig.getStorageDirectory());
        if (ksmlConfig.getApplicationServer() != null && ksmlConfig.getApplicationServer().isEnabled()) {
            streamsConfig.put(StreamsConfig.APPLICATION_SERVER_CONFIG, ksmlConfig.getApplicationServer().getApplicationServer());
        }

        if (ksmlConfig.getErrorHandling() != null) {
            ExecutionContext.INSTANCE.setConsumeHandler(getErrorHandler(ksmlConfig.getErrorHandling().getConsume(), "ConsumeError"));
            ExecutionContext.INSTANCE.setProduceHandler(getErrorHandler(ksmlConfig.getErrorHandling().getProduce(), "ProduceError"));
            ExecutionContext.INSTANCE.setProcessHandler(getErrorHandler(ksmlConfig.getErrorHandling().getProcess(), "ProcessError"));
        }

        // Set up the notation library
        final var notationLibrary = new NotationLibrary();
        notationLibrary.register(AvroNotation.NOTATION_NAME, new AvroNotation(streamsConfig), null);
        notationLibrary.register(BinaryNotation.NOTATION_NAME, new BinaryNotation(), null);
        notationLibrary.register(CsvNotation.NOTATION_NAME, new CsvNotation(), new CsvDataObjectConverter());
        notationLibrary.register(JsonNotation.NOTATION_NAME, new JsonNotation(), new JsonDataObjectConverter());
        notationLibrary.register(SOAPNotation.NOTATION_NAME, new SOAPNotation(), new SOAPDataObjectConverter());
        notationLibrary.register(XmlNotation.NOTATION_NAME, new XmlNotation(), new XmlDataObjectConverter());

        // set up a stream topology generator based on the provided KSML definition
        var ksmlConf = io.axual.ksml.KSMLConfig.builder()
                .sourceType("file")
                .configDirectory(ksmlConfig.getConfigDirectory())
                .schemaDirectory(ksmlConfig.getSchemaDirectory())
                .source(ksmlConfig.getDefinitions())
                .notationLibrary(notationLibrary)
                .build();

        // Register schema loaders
        SchemaLibrary.registerLoader(AvroNotation.NOTATION_NAME, new AvroSchemaLoader(ksmlConf.schemaDirectory()));
        SchemaLibrary.registerLoader(CsvNotation.NOTATION_NAME, new CsvSchemaLoader(ksmlConf.schemaDirectory()));
        SchemaLibrary.registerLoader(JsonNotation.NOTATION_NAME, new JsonSchemaLoader(ksmlConf.schemaDirectory()));
        SchemaLibrary.registerLoader(XmlNotation.NOTATION_NAME, new XmlSchemaLoader(ksmlConf.schemaDirectory()));

        var applicationId = kafkaConfig != null ? kafkaConfig.get(StreamsConfig.APPLICATION_ID_CONFIG) : null;
        if (applicationId == null) {
            applicationId = "ksmlApplicationId";
        }

        ExecutionContext.INSTANCE.setSerdeWrapper(
                serde -> new Serdes.WrapperSerde<>(
                        new ResolvingSerializer<>(serde.serializer(), streamsConfig),
                        new ResolvingDeserializer<>(serde.deserializer(), streamsConfig)));
        final var topologyGenerator = new TopologyGenerator(applicationId, ksmlConf);
//        final var topologyConfig = new TopologyConfig(applicationId, new StreamsConfig(streamsConfig), new Properties());
        final var topologyConfig = new TopologyConfig(new StreamsConfig(streamsConfig));
        final var streamsBuilder = new StreamsBuilder(topologyConfig);
        final var topology = topologyGenerator.create(streamsBuilder, notationLibrary, ksmlConfig.getDefinitions());

        kafkaStreams = new KafkaStreams(topology, mapToProperties(streamsConfig));
        kafkaStreams.setUncaughtExceptionHandler(ExecutionContext.INSTANCE::uncaughtException);
    }

    private ErrorHandler getErrorHandler(KSMLErrorHandlingConfig.ErrorHandlingConfig config, String loggerName) {
        if (config == null) return new ErrorHandler(true, false, loggerName, ErrorHandler.HandlerType.STOP_ON_FAIL);
        final var handlerType = switch (config.getHandler()) {
            case CONTINUE -> ErrorHandler.HandlerType.CONTINUE_ON_FAIL;
            case STOP -> ErrorHandler.HandlerType.STOP_ON_FAIL;
        };
        return new ErrorHandler(
                config.isLog(),
                config.isLogPayload(),
                config.getLoggerName() != null ? config.getLoggerName() : loggerName,
                handlerType);
    }

    private Properties mapToProperties(Map<String, Object> configs) {
        var result = new Properties();
        configs.entrySet().stream().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }

    @Override
    public State getState() {
        return convertStreamsState(kafkaStreams.state());
    }

    @Override
    public StreamsQuerier getQuerier() {
        return new StreamsQuerier() {
            @Override
            public Collection<StreamsMetadata> allMetadataForStore(String storeName) {
                return kafkaStreams.streamsMetadataForStore(storeName);
            }

            @Override
            public <K> KeyQueryMetadata queryMetadataForKey(String storeName, K key, Serializer<K> keySerializer) {
                return kafkaStreams.queryMetadataForKey(storeName, key, keySerializer);
            }

            @Override
            public <T> T store(StoreQueryParameters<T> storeQueryParameters) {
                return kafkaStreams.store(storeQueryParameters);
            }
        };
    }

    @Override
    public void close() {
        kafkaStreams.close();
    }

    @Override
    public void run() {
        log.info("Starting Kafka Backend");
        kafkaStreams.start();
        Utils.sleep(1000);
        while (!stopRunning.get()) {
            final var state = getState();
            if (state == State.STOPPED || state == State.FAILED) {
                log.info("Streams implementation has stopped, stopping Kafka Backend");
                break;
            }
            Utils.sleep(200);
        }
    }
}
