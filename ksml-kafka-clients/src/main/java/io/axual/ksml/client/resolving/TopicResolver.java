package io.axual.ksml.client.resolving;

/*-
 * ========================LICENSE_START=================================
 * Extended Kafka clients for KSML
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

import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.regex.Pattern;

public interface TopicResolver extends Resolver {
    /**
     * Translates the internal representation of topic pattern to the external one.
     *
     * @param pattern the application's internal topic pattern
     * @return the external representation of the topic pattern
     */
    default Pattern resolve(final Pattern pattern) {
        // Wrap the pattern in brackets and resolve the resulting string as if it were a topic
        String resolvedRegex = resolve("(" + pattern.pattern() + ")");
        return Pattern.compile(resolvedRegex);
    }

    /**
     * Translates the internal representation of topic name to the external one.
     *
     * @param topicPartition the application's internal topic partition
     * @return the external representation of the topic partition
     */
    default TopicPartition resolve(final TopicPartition topicPartition) {
        if (topicPartition == null) return null;
        return new TopicPartition(resolve(topicPartition.topic()), topicPartition.partition());
    }

    /**
     * Translates the internal representation of topic names to the external ones.
     *
     * @param topics the application's internal topic names
     * @return the external representation of the topics
     */
    default Set<String> resolve(Collection<String> topics) {
        if (topics == null) return new HashSet<>();

        Set<String> result = HashSet.newHashSet(topics.size());
        for (String topic : topics) {
            String resolvedTopic = resolve(topic);
            if (resolvedTopic != null) {
                result.add(resolvedTopic);
            }
        }
        return result;
    }

    /**
     * Translates the internal representation of topic partitions to the external ones.
     *
     * @param topicPartitions the application's internal topic partitions
     * @return the external representation of the topic partitions
     */
    default Set<TopicPartition> resolveTopicPartitions(Collection<TopicPartition> topicPartitions) {
        if (topicPartitions == null) return new HashSet<>();

        Set<TopicPartition> result = HashSet.newHashSet(topicPartitions.size());
        for (TopicPartition partition : topicPartitions) {
            result.add(resolve(partition));
        }
        return result;
    }

    /**
     * Translates the internal representation of a topic partition map to the external one.
     *
     * @param <V>               any type used as Value in the Map
     * @param topicPartitionMap the map containing the application's internal topic partitions as
     *                          Keys
     * @return the map containing the external representation of the topic partitions as Keys
     */
    default <V> Map<TopicPartition, V> resolve(Map<TopicPartition, V> topicPartitionMap) {
        if (topicPartitionMap == null) return new HashMap<>();

        Map<TopicPartition, V> result = HashMap.newHashMap(topicPartitionMap.size());
        for (Map.Entry<TopicPartition, V> entry : topicPartitionMap.entrySet()) {
            result.put(resolve(entry.getKey()), entry.getValue());
        }
        return result;
    }

    /**
     * Translates the internal representation of a topic partition map to the external one.
     *
     * @param topics the map containing the application's internal topic partitions as Keys
     * @return the map containing the external representation of the topic partitions as Keys
     */
    default TopicCollection resolve(TopicCollection topics) {
        if (topics instanceof TopicCollection.TopicNameCollection topicNames) {
            return TopicCollection.ofTopicNames(resolve(topicNames.topicNames()));
        }
        return topics;
    }

    /**
     * Translates the external representation of topic partition to the internal one.
     *
     * @param topicPartition the external topic partition
     * @return the internal consumer topic partition
     */
    default TopicPartition unresolve(TopicPartition topicPartition) {
        if (topicPartition == null) return null;

        String unresolvedTopic = unresolve(topicPartition.topic());
        if (unresolvedTopic == null) return null;
        return new TopicPartition(unresolvedTopic, topicPartition.partition());
    }

    /**
     * Translates the external representation of topic names to the internal ones.
     *
     * @param topics the external topic names
     * @return the internal consumer topic names
     */
    default Set<String> unresolve(Collection<String> topics) {
        if (topics == null) return new HashSet<>();

        Set<String> result = HashSet.newHashSet(topics.size());
        for (String topic : topics) {
            String unresolvedTopic = unresolve(topic);
            if (unresolvedTopic != null) {
                result.add(unresolvedTopic);
            }
        }
        return result;
    }

    /**
     * Translates the external representation of topic partitions to the internal ones.
     *
     * @param topicPartitions the external topic partitions
     * @return the internal consumer topic partitions
     */
    default Set<TopicPartition> unresolveTopicPartitions(Collection<TopicPartition> topicPartitions) {
        if (topicPartitions == null) return new HashSet<>();

        Set<TopicPartition> result = HashSet.newHashSet(topicPartitions.size());
        for (TopicPartition partition : topicPartitions) {
            TopicPartition unresolvedPartition = unresolve(partition);
            if (unresolvedPartition != null) {
                result.add(unresolvedPartition);
            }
        }
        return result;
    }

    /**
     * Translates the external representation of topic partition to the internal one.
     *
     * @param <V>               any type used as Value in the Map
     * @param topicPartitionMap the map containing the application's external topic partitions as
     *                          keys
     * @return the map containing the internal representation of the topic partitions as keys
     */
    default <V> Map<TopicPartition, V> unresolve(Map<TopicPartition, V> topicPartitionMap) {
        if (topicPartitionMap == null) return new HashMap<>();

        Map<TopicPartition, V> result = HashMap.newHashMap(topicPartitionMap.size());
        for (Map.Entry<TopicPartition, V> entry : topicPartitionMap.entrySet()) {
            TopicPartition unresolvedTopic = unresolve(entry.getKey());
            if (unresolvedTopic != null) {
                result.put(unresolvedTopic, entry.getValue());
            }
        }
        return result;
    }
}
