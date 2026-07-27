package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * KSML Kafka clients
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

import io.axual.ksml.client.testutil.PrefixResolver;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the default methods of {@link TopicResolver} through a simple prefixing test double.
 * The double resolves by prefixing and unresolves by stripping the prefix, returning {@code null}
 * for names that do not carry the prefix so the null-filtering branches are covered too.
 */
class TopicResolverTest {
    private static final String PREFIX = PrefixResolver.PREFIX;

    private final TopicResolver resolver = new PrefixResolver();

    @Test
    @DisplayName("Resolve wraps and prefixes a topic pattern")
    void resolvePattern() {
        final var resolved = resolver.resolve(Pattern.compile("topic.*"));
        assertThat(resolved.pattern()).isEqualTo(PREFIX + "(topic.*)");
    }

    @Test
    @DisplayName("Resolve and unresolve of a topic partition preserve the partition number")
    void resolveTopicPartition() {
        final var partition = new TopicPartition("orders", 3);
        final var resolved = resolver.resolve(partition);
        assertThat(resolved).isEqualTo(new TopicPartition(PREFIX + "orders", 3));
        assertThat(resolver.unresolve(resolved)).isEqualTo(partition);
    }

    @Test
    @DisplayName("Null topic partitions resolve and unresolve to null")
    void nullTopicPartition() {
        assertThat(resolver.resolve((TopicPartition) null)).isNull();
        assertThat(resolver.unresolve((TopicPartition) null)).isNull();
    }

    @Test
    @DisplayName("Unresolve of an unknown topic partition returns null")
    void unresolveUnknownTopicPartition() {
        assertThat(resolver.unresolve(new TopicPartition("unprefixed", 0))).isNull();
    }

    @Test
    @DisplayName("Resolve and unresolve of topic collections filter unresolvable entries")
    void resolveTopicCollections() {
        assertThat(resolver.resolve(List.of("a", "b"))).containsExactlyInAnyOrder(PREFIX + "a", PREFIX + "b");
        assertThat(resolver.unresolve(List.of(PREFIX + "a", "unprefixed"))).containsExactly("a");
    }

    @Test
    @DisplayName("Null topic collections resolve to empty sets")
    void nullTopicCollections() {
        assertThat(resolver.resolve((java.util.Collection<String>) null)).isEmpty();
        assertThat(resolver.unresolve((java.util.Collection<String>) null)).isEmpty();
    }

    @Test
    @DisplayName("Resolve and unresolve of topic partition sets")
    void resolveTopicPartitionSets() {
        final var partitions = Set.of(new TopicPartition("a", 0), new TopicPartition("b", 1));
        final var resolved = resolver.resolveTopicPartitions(partitions);
        assertThat(resolved).containsExactlyInAnyOrder(new TopicPartition(PREFIX + "a", 0), new TopicPartition(PREFIX + "b", 1));
        assertThat(resolver.unresolveTopicPartitions(resolved)).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    @DisplayName("Null topic partition sets resolve to empty sets")
    void nullTopicPartitionSets() {
        assertThat(resolver.resolveTopicPartitions(null)).isEmpty();
        assertThat(resolver.unresolveTopicPartitions(null)).isEmpty();
    }

    @Test
    @DisplayName("Unresolve of topic partition set drops unresolvable partitions")
    void unresolveTopicPartitionSetFilters() {
        final var partitions = Set.of(new TopicPartition(PREFIX + "a", 0), new TopicPartition("unprefixed", 1));
        assertThat(resolver.unresolveTopicPartitions(partitions)).containsExactly(new TopicPartition("a", 0));
    }

    @Test
    @DisplayName("Resolve and unresolve of topic partition keyed maps")
    void resolveTopicPartitionMap() {
        final var map = Map.of(new TopicPartition("a", 0), 100L);
        final var resolved = resolver.resolve(map);
        assertThat(resolved).containsExactly(Map.entry(new TopicPartition(PREFIX + "a", 0), 100L));
        assertThat(resolver.unresolve(resolved)).containsExactly(Map.entry(new TopicPartition("a", 0), 100L));
    }

    @Test
    @DisplayName("Null topic partition maps resolve to empty maps")
    void nullTopicPartitionMap() {
        assertThat(resolver.resolve((Map<TopicPartition, Long>) null)).isEmpty();
        assertThat(resolver.unresolve((Map<TopicPartition, Long>) null)).isEmpty();
    }

    @Test
    @DisplayName("Unresolve of a topic partition map drops unresolvable keys")
    void unresolveTopicPartitionMapFilters() {
        final var map = Map.of(new TopicPartition(PREFIX + "a", 0), 1L, new TopicPartition("unprefixed", 1), 2L);
        assertThat(resolver.unresolve(map)).containsExactly(Map.entry(new TopicPartition("a", 0), 1L));
    }

    @Test
    @DisplayName("A topic name collection is resolved by name")
    void resolveTopicNameCollection() {
        final var resolved = resolver.resolve(TopicCollection.ofTopicNames(List.of("orders")));
        assertThat(resolved).isInstanceOfSatisfying(TopicCollection.TopicNameCollection.class,
                names -> assertThat(names.topicNames()).containsExactly(PREFIX + "orders"));
    }

    @Test
    @DisplayName("A topic id collection is passed through unchanged")
    void resolveTopicIdCollection() {
        final var ids = TopicCollection.ofTopicIds(List.of(Uuid.randomUuid()));
        assertThat(resolver.resolve(ids)).isSameAs(ids);
    }
}
