package io.axual.ksml.runner.producer;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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

import io.axual.ksml.data.notation.binary.BinaryNotation;
import io.axual.ksml.data.notation.json.JsonNotation;
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
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
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
    @DisplayName("Messages rejected by the strategy are not added to the result")
    void rejectedMessagesAreNotCollected() {
        final var rejectingStrategy = mock(ProducerStrategy.class);
        when(rejectingStrategy.validateMessage(any(), any())).thenReturn(false);
        final var tuple = new DataTuple(new DataString("k"), new DataString("v"));

        // Through extractMessages a rejected (null) message must not be added.
        assertThat(ExecutableProducer.extractMessages(tuple, STRING_TYPE, STRING_TYPE, rejectingStrategy)).isEmpty();
    }

    @Test
    @DisplayName("A message rejected by the producer strategy is skipped")
    void skipsMessageRejectedByStrategy() {
        final var rejectingStrategy = mock(ProducerStrategy.class);
        when(rejectingStrategy.validateMessage(any(), any())).thenReturn(false);
        final var tuple = new DataTuple(new DataString("k"), new DataString("v"));

        assertThat(ExecutableProducer.shapeMessage(tuple, STRING_TYPE, STRING_TYPE, rejectingStrategy)).isNull();
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

    /** A "produce once" strategy (no interval/count/until), so no Python predicates are needed. */
    private static ProducerStrategy onceStrategy(MetricTags tags) {
        final var definition = new ProducerDefinition(null, null, null, null, null, null, null);
        return new ProducerStrategy(null, "ns", "gen", tags, definition);
    }

    private static Serializer<Object> passthroughSerializer() {
        return (topic, data) -> new byte[]{1, 2, 3};
    }

    private static ExecutableProducer executableProducerFor(DataObject generated) {
        final var tags = new MetricTags();
        return new ExecutableProducer(
                generatorReturning(generated),
                onceStrategy(tags),
                tags,
                "output-topic",
                STRING_TYPE,
                STRING_TYPE,
                null, // no partitioner
                passthroughSerializer(),
                passthroughSerializer());
    }

    @Test
    @DisplayName("produceMessages generates, serializes and sends one record for a single-shot producer")
    void produceMessagesSendsRecord() {
        final var producer = executableProducerFor(new DataTuple(new DataString("key"), new DataString("value")));
        final var mockProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());

        producer.produceMessages(mockProducer);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0).topic()).isEqualTo("output-topic");
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

    @Test
    @DisplayName("name and interval are exposed from the generator and strategy")
    void exposesNameAndInterval() {
        final var producer = executableProducerFor(new DataTuple(new DataString("k"), new DataString("v")));

        assertThat(producer.name()).isEqualTo("gen");
        // A single-shot producer has a zero interval.
        assertThat(producer.interval()).isZero();
    }
}
