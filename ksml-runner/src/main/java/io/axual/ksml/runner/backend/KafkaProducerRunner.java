package io.axual.ksml.runner.backend;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.runner.config.KSMLConfig;
import io.axual.ksml.runner.streams.KSMLClientSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class KafkaProducerRunner implements Runner {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerRunner.class);
    private static final IntervalSchedule<ExecutableProducer> schedule = new IntervalSchedule<>();
    private final Map<String, JsonNode> definitions;
    private final Producer<byte[],byte[]> producer;

    public KafkaProducerRunner(Map<String, JsonNode> definitions, Map<String,Object> kafkaConfigs) {
        this.definitions = definitions;
        producer = new KafkaProducer<byte[], byte[]>(kafkaConfigs);
    }

    private static Map<String, String> getGenericConfigs() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ACKS_CONFIG, "1");
        configs.put(RETRIES_CONFIG, "0");
        configs.put(RETRY_BACKOFF_MS_CONFIG, "1000");
        configs.put(RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000");
        configs.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "10");
        configs.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        configs.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        configs.put("specific.avro.reader", "true");
        return configs;
    }

    public void run() {
        log.info("Initializing Kafka backend");
        final var configs = new HashMap<>(getGenericConfigs());
        configs.putAll(config.getKafka());
        final var factory = new KSMLClientSupplier(configs);

        // Read all producer definitions from the configured YAML files
        var notationLibrary = factory.getNotationLibrary();
        var context = new PythonContext(new DataObjectConverter(notationLibrary));
        var producers = new ProducerDefinitionFileParser(config.getKsml()).create(notationLibrary, context);

        // Load all functions into the Python context

        // Schedule all defined producers
        for (var entry : producers.entrySet()) {
            var target = entry.getValue().target();
            var name = entry.getKey();
            var gen = entry.getValue().generator();
            final var generator = gen.name() != null
                    ? PythonFunction.fromNamed(context, gen.name(), gen.definition())
                    : PythonFunction.fromAnon(context, name, gen.definition(), "ksml.generator." + name);
            var cond = entry.getValue().condition();
            final var condition = (cond != null && cond.definition() != null)
                    ? cond.name() != null
                    ? PythonFunction.fromNamed(context, cond.name(), cond.definition())
                    : PythonFunction.fromAnon(context, name, cond.definition(), "ksml.condition." + name)
                    : null;
            var keySerde = notationLibrary.get(target.keyType.notation()).getSerde(target.keyType.dataType(), true);
            var keySerializer = factory.wrapSerializer(keySerde.serializer());
            var valueSerde = notationLibrary.get(target.valueType.notation()).getSerde(target.valueType.dataType(), false);
            var valueSerializer = factory.wrapSerializer(valueSerde.serializer());
            var ep = new ExecutableProducer(notationLibrary, generator, condition, target.topic, target.keyType, target.valueType, keySerializer, valueSerializer);
            schedule.schedule(entry.getValue().interval().toMillis(), ep);
            log.info("Scheduled producers: {}", entry.getKey());
        }

        try (final Producer<byte[], byte[]> producer = factory.getProducer()) {
            var interrupted = false;

            while (!interrupted) {
                try {
                    var generator = schedule.getScheduledItem();
                    while (generator != null) {
                        generator.produceMessage(producer);
                        generator = schedule.getScheduledItem();
                    }
                    Utils.sleep(10);
                } catch (Exception e) {
                    log.info("Interrupted: {}", e.getMessage());
                    e.printStackTrace();
                    interrupted = true;
                }
            }
        } finally {
            log.info("Done!");
        }
    }
}
