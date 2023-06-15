package io.axual.ksml.store;

public class StreamsExample {
    private static void myMain() {
//        final TopologyFactory topologyFactory = builder -> {
//            builder.addStateStore(Stores.keyValueStoreBuilder(
//                    Stores.persistentKeyValueStore("offsets-prev-window"),
//                    Serdes.String(),
//                    Serdes.Long()));
//
//            KStream<String, String> source = builder.stream("metrics");
//
//            source.process(new ProcessorSupplier<String, String, String, Long>() {
//                        @Override
//                        public Processor<String, String, String, Long> get() {
//                            return new Processor<>() {
//
//                                private ProcessorContext<String, Long> context;
//                                private KeyValueStore<String, Long> state;
//
//                                @SuppressWarnings("unchecked")
//                                @Override
//                                public void init(ProcessorContext<String, Long> context) {
//                                    this.context = context;
//                                    state = context.getStateStore("offsets-prev-window");
//                                }
//
//                                @Override
//                                public void process(org.apache.kafka.streams.processor.api.Record<String, String> record) {
//                                    String topicPartition = extractTopicPartitionFromValue(record.value());
//                                    Long newOffset = extractOffsetFromValue(record.value());
//                                    Long oldOffset = state.get(topicPartition);
//                                    state.put(topicPartition, newOffset);
//                                    if (oldOffset != null) {
//                                        this.context.forward(new Record<>(topicPartition, newOffset - oldOffset, record.timestamp()));
//                                    }
//                                }
//
//                                @Override
//                                public void close() {
//                                    // do nothing
//                                }
//                            };
//                        }
//                    }, "offsets-prev-window")
//                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
//                    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
//                    .reduce(Long::sum, Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("messages-running-total-within-window")
//                            .withKeySerde(Serdes.String())
//                            .withValueSerde(Serdes.Long()))
//                    .toStream()
//                    .map((key, value) -> {
//                        String topicPartition = extractTopicPartitionFromKey(key.key());
//                        String windowStart = Instant.ofEpochMilli(key.window().start()).toString();
//                        String windowEnd = Instant.ofEpochMilli(key.window().end()).toString();
//                        if (value < 0L) { // when topic is deleted and recreated
//                            value = 0L;
//                        }
//                        return KeyValue.pair(topicPartition + "@" + windowStart + "-" + windowEnd, value);
//                    })
//                    .peek((key, value) -> LOG.info("key {}, value {}", key, value))
//                    .to("usage", Produced.with(Serdes.String(), Serdes.Long()));
//
//            return builder.build();
//        };
    }
}
