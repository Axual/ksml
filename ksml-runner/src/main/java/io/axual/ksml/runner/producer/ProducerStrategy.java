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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.ProducerDefinition;
import io.axual.ksml.metric.MetricTags;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.user.UserPredicate;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class ProducerStrategy {
    private static final int INFINITE = -1;

    private final boolean once;
    private final long messageCount;
    private final long batchSize;
    @Getter
    private final Duration interval;
    private final UserPredicate condition;
    private final UserPredicate until;
    @Getter
    private long messagesProduced = 0;
    private boolean untilTriggered = false;

    public ProducerStrategy(PythonContext context, String namespace, String name, MetricTags tags, ProducerDefinition definition) {
        // If interval, messageCount and until are not defined, then only produce one message
        once = (definition.interval() == null && definition.messageCount() == null && definition.until() == null);
        if (once) {
            messageCount = 1;
            batchSize = 1;
            interval = Duration.ofMillis(0);
            condition = null;
            until = null;
            return;
        }

        // Set the message count
        messageCount = definition.messageCount() != null
                ? definition.messageCount() >= 1
                ? definition.messageCount()
                : 1
                : INFINITE;

        // Set the batch size
        if (definition.batchSize() != null) {
            if (definition.batchSize() >= 1 && definition.batchSize() <= 1000) {
                batchSize = definition.batchSize();
            } else {
                log.warn("Batch size must be between 1 and 1000. Using 1 as default.");
                batchSize = 1;
            }
        } else {
            batchSize = 1;
        }

        // Set the interval
        interval = definition.interval() != null ? definition.interval() : Duration.ofMillis(0);

        // Set up the message validator
        condition = userPredicateFrom(definition.condition(), context, namespace, name, tags);

        // Set up the user function that - when returns true - halts the producer
        until = userPredicateFrom(definition.until(), context, namespace, name, tags);
    }

    private UserPredicate userPredicateFrom(FunctionDefinition function, PythonContext context, String namespace, String name, MetricTags tags) {
        return function != null
                ? function.name() != null
                ? new UserPredicate(PythonFunction.forPredicate(context, namespace, function.name(), function), tags)
                : new UserPredicate(PythonFunction.forPredicate(context, namespace, name, function), tags)
                : null;
    }

    public boolean shouldReschedule() {
        // Reschedule if not once, not all messages were produced yet and until never returned true
        return !once && (messageCount == INFINITE || messagesProduced < messageCount) && !untilTriggered;
    }

    public boolean validateMessage(DataObject key, DataObject value) {
        return (condition == null || condition.test(key, value));
    }

    public boolean continueAfterMessage(DataObject key, DataObject value) {
        if (until != null && until.test(key, value)) {
            untilTriggered = true;
            return false;
        }
        return true;
    }

    public long batchSize() {
        if (messageCount == INFINITE) return batchSize;
        return Math.min(batchSize, messageCount - messagesProduced);
    }

    public void successfullyProducedOneMessage() {
        messagesProduced++;
    }
}
