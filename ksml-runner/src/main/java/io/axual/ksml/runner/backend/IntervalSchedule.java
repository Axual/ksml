package io.axual.ksml.runner.backend;

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

import com.google.common.primitives.Ints;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Scheduler for {@link ExecutableProducer}s.
 */
public class IntervalSchedule {
    private final DelayQueue<ScheduledProducer> scheduledProducers = new DelayQueue<>();

    /**
     * Schedule a producer for immediate return.
     *
     * @param producer a {@link ExecutableProducer}.
     */
    public void schedule(ExecutableProducer producer) {
        scheduledProducers.put(new ScheduledProducer(producer, System.currentTimeMillis()));
    }

    /**
     * Schedule a producer to be returned after the specified waiting time.
     *
     * @param producer a producer to schedule.
     * @param startTime for when the producer is scheduled to execute.
     */
    public void schedule(ExecutableProducer producer, long startTime) {
        scheduledProducers.put(new ScheduledProducer(producer, startTime));
    }

    /**
     * Return the next scheduled {@link ExecutableProducer}.
     * This method will block at most 10 ms waiting for a producer to return.
     *
     * @return the next available ScheduledProducer, or <code>null</code> if none available (yet).
     */
    public ScheduledProducer getScheduledItem() {
        try {
            return scheduledProducers.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public boolean hasScheduledItems() {
        return !scheduledProducers.isEmpty();
    }

    /**
     * Inner data class to keep a scheduled producer and the time it should be returned.
     */
    public record ScheduledProducer(ExecutableProducer producer, long startTime) implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            long diff = startTime - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Ints.saturatedCast(
                    this.startTime - ((ScheduledProducer) other).startTime);
        }
    }
}
