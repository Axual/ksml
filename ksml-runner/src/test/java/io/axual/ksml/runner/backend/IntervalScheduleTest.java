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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class IntervalScheduleTest {

    private IntervalSchedule<TestItem> intervalSchedule;

    @BeforeEach
    void setup() {
        intervalSchedule = new IntervalSchedule<>();
    }

    @Test
    @DisplayName("An item can be scheduled repeatedly")
    void scheduledOnInterval() throws InterruptedException {
        // if we schedule with interval 200ms
        intervalSchedule.schedule(new TestItem("test", 200L));

        // at first the item is returned straight away
        assertEquals("test", intervalSchedule.getScheduledItem().value);

        // then for the duration of the interval, nothing is returned
        assertNull(intervalSchedule.getScheduledItem(), "should not return item before interval");

        // after the interval expires, the same item is returned again
        Thread.sleep(500);
        assertEquals("test", intervalSchedule.getScheduledItem().value);
    }

    @Test
    @DisplayName("An item can be scheduled single shot")
    void singleShot() throws InterruptedException {
        // if we schedule without interval (will set default to 0 internally)
        intervalSchedule.schedule(new TestItem("test", RescheduleStrategy.once()));

        // the item is returned straight away
        assertEquals("test", intervalSchedule.getScheduledItem().value);

        // but it is not rescheduled
        Thread.sleep(100);
        assertNull(intervalSchedule.getScheduledItem(), "item should be returned only once");
    }

    @Test
    @DisplayName("Order of scheduling is maintained for equal interval")
    void maintainsOrdering() throws InterruptedException {
        // if we schedule two items with same timeout
        intervalSchedule.schedule(new TestItem("test1", 500));
        intervalSchedule.schedule(new TestItem("test2", 500));

        // when they are retrieved, insertion order is maintained
        assertEquals("test1", intervalSchedule.getScheduledItem().value);
        assertEquals("test2", intervalSchedule.getScheduledItem().value);
        assertNull(intervalSchedule.getScheduledItem(), "should return no more items at this point");

        // after the interval expires, the same items are returned in the same order
        Thread.sleep(600);
        assertEquals("test1", intervalSchedule.getScheduledItem().value);
        assertEquals("test2", intervalSchedule.getScheduledItem().value);
        assertNull(intervalSchedule.getScheduledItem(), "should return no more items at this point");
    }

    record TestItem(String value, RescheduleStrategy rescheduleStrategy) implements RescheduleStrategy {
        TestItem(String value, long interval) {
            this(value, RescheduleStrategy.always(Duration.ofMillis(interval)));
        }

        @Override
        public boolean shouldReschedule() {
            return this.rescheduleStrategy.shouldReschedule();
        }

        @Override
        public Duration interval() {
            return this.rescheduleStrategy.interval();
        }

    }
}
