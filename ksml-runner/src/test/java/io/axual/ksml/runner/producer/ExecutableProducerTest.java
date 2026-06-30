package io.axual.ksml.runner.producer;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.definition.ParameterDefinition;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.execution.ExecutionContext;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.store.StateStores;
import io.axual.ksml.type.UserType;
import io.axual.ksml.user.UserFunction;
import io.axual.ksml.user.UserGenerator;
import io.axual.ksml.user.UserStreamPartitioner;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the message-shaping logic of {@link ExecutableProducer} ({@code extractMessages} and
 * {@code shapeMessage}), which is the pure, Python-free part of the producer. The end-to-end producing
 * path (generator execution, serialization, Kafka send) is covered by {@code KafkaProducerRunnerTest}
 * on GraalVM.
 */
class ExecutableProducerTest {

    private static final UserType STRING_TYPE = new UserType(DataString.DATATYPE);
    private static final String TOPIC = "output-topic";

    @BeforeEach
    void registerNotations() {
        // DataObjectConverter.convert resolves notations from the ExecutionContext, so the default
        // notation must be registered for the type conversions inside shapeMessage to work.
        final var jsonNotation = new JsonNotation();
        ExecutionContext.INSTANCE.notationLibrary().register(JsonNotation.NOTATION_NAME, jsonNotation);
        final var binaryNotation = new BinaryNotation(jsonNotation::serde);
        ExecutionContext.INSTANCE.notationLibrary().register(UserType.DEFAULT_NOTATION, binaryNotation);
    }

    private static ProducerStrategy acceptingStrategy() {
        final var strategy = mock(ProducerStrategy.class);
        when(strategy.validateMessage(any(), any())).thenReturn(true);
        return strategy;
    }

    @Test
    @DisplayName("A valid (key, value) tuple is shaped into a single message")
    void shapesSingleTuple() {
        final var tuple = new DataTuple(new DataString("key-1"), new DataString("value-1"));

        final var messages = ExecutableProducer.extractMessages(tuple, STRING_TYPE, STRING_TYPE, acceptingStrategy());

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).left()).isEqualTo(new DataString("key-1"));
        assertThat(messages.get(0).right()).isEqualTo(new DataString("value-1"));
    }

    @Test
    @DisplayName("A list of tuples yields one message per valid element and skips invalid ones")
    void shapesListSkippingInvalidElements() {
        final var list = new DataList(); // DataType.UNKNOWN accepts mixed element types
        list.add(new DataTuple(new DataString("k1"), new DataString("v1")));
        list.add(new DataString("not-a-tuple"));                              // skipped
        list.add(new DataTuple(new DataString("k2"), new DataString("v2")));

        final var messages = ExecutableProducer.extractMessages(list, STRING_TYPE, STRING_TYPE, acceptingStrategy());

        assertThat(messages).hasSize(2);
    }

    @Test
    @DisplayName("A tuple that does not have exactly two elements produces no message")
    void ignoresTupleWithWrongArity() {
        final var tuple = new DataTuple(new DataString("only-one"));

        final var messages = ExecutableProducer.extractMessages(tuple, STRING_TYPE, STRING_TYPE, acceptingStrategy());

        assertThat(messages).isEmpty();
    }

    @Test
    @DisplayName("A generated object that is neither a tuple nor a list yields no messages")
    void ignoresUnsupportedGeneratedType() {
        final var messages = ExecutableProducer.extractMessages(new DataString("scalar"), STRING_TYPE, STRING_TYPE, acceptingStrategy());

        assertThat(messages).isEmpty();
    }

    @Test
    @DisplayName("A list element that is a tuple with the wrong arity is skipped")
    void skipsListElementWithWrongArity() {
        final var list = new DataList();
        list.add(new DataTuple(new DataString("k1"), new DataString("v1"))); // valid
        list.add(new DataTuple(new DataString("only-one")));                 // wrong arity, skipped

        final var messages = ExecutableProducer.extractMessages(list, STRING_TYPE, STRING_TYPE, acceptingStrategy());

        assertThat(messages).hasSize(1);
    }

    @Test
    @DisplayName("A message rejected by the producer strategy is not collected")
    void rejectedMessageIsNotCollected() {
        final var rejectingStrategy = mock(ProducerStrategy.class);
        when(rejectingStrategy.validateMessage(any(), any())).thenReturn(false);
        final var tuple = new DataTuple(new DataString("k"), new DataString("v"));

        // extractMessages -> addShapedMessage -> shapeMessage returns null for a rejected tuple, so
        // nothing is added to the result. This drives both the shapeMessage rejection path and the
        // null-guard in addShapedMessage.
        assertThat(ExecutableProducer.extractMessages(tuple, STRING_TYPE, STRING_TYPE, rejectingStrategy)).isEmpty();
    }

    @Test
    @DisplayName("A validated tuple is converted to the configured key/value types")
    void shapeMessageConvertsToTargetTypes() {
        final var tuple = new DataTuple(new DataString("k"), new DataString("v"));

        final var shaped = ExecutableProducer.shapeMessage(tuple, STRING_TYPE, STRING_TYPE, acceptingStrategy());

        assertThat(shaped).isNotNull();
        assertThat(shaped.left()).isEqualTo(new DataString("k"));
        assertThat(shaped.right()).isEqualTo(new DataString("v"));
    }

    // ---- instance-level produce tests (stubbed generator, no Python/GraalVM) -------------------

    /** A generator UserFunction whose {@code call} returns a fixed value, avoiding the Python runtime. */
    private static UserFunction generatorReturning(DataObject result) {
        // A generator must declare the tuple/list-of-tuples result type expected by UserGenerator.
        final var generatorResultType = new UserType(UserGenerator.EXPECTED_RESULT_TYPE);
        return new UserFunction("ns", "gen", new ParameterDefinition[0], generatorResultType, List.of()) {
            @Override
            public DataObject call(StateStores stores, DataObject... parameters) {
                return result;
            }
        };
    }

    /** A valid (key, value) tuple used by the produce-path tests. */
    private static DataTuple sampleTuple() {
        return new DataTuple(new DataString("key"), new DataString("value"));
    }

    /** A "produce once" strategy (no interval/count/until), so no Python predicates are needed. */
    private static ProducerStrategy onceStrategy(MetricTags tags) {
        final var definition = new ProducerDefinition(null, null, null, null, null, null, null);
        return new ProducerStrategy(null, "ns", "gen", tags, definition);
    }

    private static Serializer<Object> passthroughSerializer() {
        return (topic, data) -> new byte[]{1, 2, 3};
    }

    private static ExecutableProducer executableProducerFor(DataObject generated) {
        return executableProducerFor(generated, null);
    }

    private static ExecutableProducer executableProducerFor(DataObject generated, UserFunction partitioner) {
        final var tags = new MetricTags();
        final var target = new ExecutableProducer.ProducerTarget(
                TOPIC, STRING_TYPE, STRING_TYPE, passthroughSerializer(), passthroughSerializer());
        return new ExecutableProducer(
                generatorReturning(generated),
                onceStrategy(tags),
                tags,
                partitioner,
                target);
    }

    /**
     * A partitioner UserFunction that returns a fixed value, avoiding the Python runtime. The value is
     * interpreted by {@link UserStreamPartitioner}: a {@link DataInteger} selects a single partition, a
     * {@link DataList} of integers selects several, anything else selects none.
     */
    private static UserFunction partitionerReturning(DataObject result) {
        final var resultType = new UserType(UserStreamPartitioner.EXPECTED_RESULT_TYPE);
        final var params = new ParameterDefinition[]{
                new ParameterDefinition("topic", DataString.DATATYPE),
                new ParameterDefinition("key", DataString.DATATYPE),
                new ParameterDefinition("value", DataString.DATATYPE),
                new ParameterDefinition("numPartitions", DataInteger.DATATYPE)
        };
        return new UserFunction("ns", "partitioner", params, resultType, List.of()) {
            @Override
            public DataObject call(StateStores stores, DataObject... parameters) {
                return result;
            }
        };
    }

    @Test
    @DisplayName("produceMessages sends one record to the single partition chosen by the partitioner")
    void produceMessagesSendsToPartitionerSelectedPartition() {
        final var producer = executableProducerFor(
                sampleTuple(),
                partitionerReturning(new DataInteger(3)));
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        producer.produceMessages(mockProducer);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0).partition()).isEqualTo(3);
    }

    @Test
    @DisplayName("produceMessages sends one record per partition for a multi-partition partitioner")
    void produceMessagesSendsToEveryPartition() {
        final var partitions = new DataList(DataInteger.DATATYPE);
        partitions.add(new DataInteger(0));
        partitions.add(new DataInteger(2));
        final var producer = executableProducerFor(
                sampleTuple(),
                partitionerReturning(partitions));
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        producer.produceMessages(mockProducer);

        assertThat(mockProducer.history())
                .hasSize(2)
                .extracting(ProducerRecord::partition)
                .containsExactlyInAnyOrder(0, 2);
    }

    @Test
    @DisplayName("produceMessages sends nothing when the partitioner selects no partition")
    void produceMessagesSendsNothingWhenNoPartitionSelected() {
        final var producer = executableProducerFor(
                sampleTuple(),
                partitionerReturning(new DataString("not-a-partition")));
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        producer.produceMessages(mockProducer);

        assertThat(mockProducer.history()).isEmpty();
    }

    @Test
    @DisplayName("produceMessages wraps a failed send in an ExecutionException")
    void produceMessagesWrapsSendFailure() {
        final var executableProducer = executableProducerFor(new DataTuple(new DataString("k"), new DataString("v")));
        final Producer<byte[], byte[]> producer = mock();
        when(producer.send(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("send failed")));

        assertThatThrownBy(() -> executableProducer.produceMessages(producer))
                .isInstanceOf(io.axual.ksml.exception.ExecutionException.class)
                .hasMessageContaining("Could not produce to topic");
    }

    @Test
    @DisplayName("produceMessages does not count a message whose metadata has no offset")
    void produceMessagesDoesNotCountMessageWithoutOffset() {
        final var tags = new MetricTags();
        final var strategy = onceStrategy(tags);
        final var target = new ExecutableProducer.ProducerTarget(
                TOPIC, STRING_TYPE, STRING_TYPE, passthroughSerializer(), passthroughSerializer());
        final var executableProducer = new ExecutableProducer(
                generatorReturning(new DataTuple(new DataString("k"), new DataString("v"))),
                strategy, tags, null, target);

        final Producer<byte[], byte[]> producer = mock();
        final var metadata = mock(RecordMetadata.class);
        when(metadata.hasOffset()).thenReturn(false);
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(metadata));

        executableProducer.produceMessages(producer);

        // The metadata has no offset, so the message is logged as an error and not counted as produced.
        assertThat(strategy.messagesProduced()).isZero();
    }

    @Test
    @DisplayName("produceMessages does not count a message whose metadata is null")
    void produceMessagesDoesNotCountNullMetadata() {
        final var tags = new MetricTags();
        final var strategy = onceStrategy(tags);
        final var target = new ExecutableProducer.ProducerTarget(
                TOPIC, STRING_TYPE, STRING_TYPE, passthroughSerializer(), passthroughSerializer());
        final var executableProducer = new ExecutableProducer(
                generatorReturning(new DataTuple(new DataString("k"), new DataString("v"))),
                strategy, tags, null, target);

        final Producer<byte[], byte[]> producer = mock();
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(null));

        executableProducer.produceMessages(producer);

        // Null metadata is treated as an error (logged), not counted, and must not throw.
        assertThat(strategy.messagesProduced()).isZero();
    }

    @Test
    @DisplayName("produceMessages generates, serializes and sends one record for a single-shot producer")
    void produceMessagesSendsRecord() {
        final var producer = executableProducerFor(sampleTuple());
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        producer.produceMessages(mockProducer);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0).topic()).isEqualTo(TOPIC);
        // A single-shot producer should not ask to be rescheduled.
        assertThat(producer.shouldReschedule()).isFalse();
    }

    @Test
    @DisplayName("produceMessages sends nothing when the generator never yields a valid message")
    void produceMessagesSkipsWhenNoValidMessage() {
        // A scalar (non-tuple) generated value is never a valid message, so after the retries nothing is sent.
        final var producer = executableProducerFor(new DataString("not-a-tuple"));
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        producer.produceMessages(mockProducer);

        assertThat(mockProducer.history()).isEmpty();
    }
}
