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

import io.axual.ksml.runner.producer.ExecutableProducer;
import io.axual.ksml.runner.producer.IntervalSchedule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class IntervalScheduleTest {

    @Mock
    private ExecutableProducer one;
    @Mock
    private ExecutableProducer two;

    private IntervalSchedule scheduler;

    @BeforeEach
    void setup() {
        scheduler = new IntervalSchedule();
    }

    @Test
    @DisplayName("an item becomes available after the interval")
    void returnsAfterInterval() throws InterruptedException {
        // when an item is scheduled in the future
        scheduler.schedule(one, System.currentTimeMillis() + 300);

        // at first it's not returned
        assertNull(scheduler.getScheduledItem());

        // after the wait time expires, it is returned
        Thread.sleep(400);
        assertNotNull(scheduler.getScheduledItem());
    }

    @Test
    @DisplayName("the item with the shortest wait time is returned first")
    void itemsOrderedOnTime() throws InterruptedException {
        // when two items are scheduled with different wait times
        long now = System.currentTimeMillis();
        scheduler.schedule(one, now + 200);
        scheduler.schedule(two, now + 100);

        // and we wait until the longest of the timeouts expire
        Thread.sleep(600);

        // the item that had the shortest wait time is returned first
        assertEquals(two, scheduler.getScheduledItem().producer());
        assertEquals(one, scheduler.getScheduledItem().producer());
        assertNull(scheduler.getScheduledItem());
    }

    @Test
    @DisplayName("An item can be scheduled for immediate return")
    void intervalDefaultsZero() {
        // given a producer scheduled without an interval
        scheduler.schedule(one);

        // it will be returned straight away
        assertNotNull(scheduler.getScheduledItem());
    }
}
