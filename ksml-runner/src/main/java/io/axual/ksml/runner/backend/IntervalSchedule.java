package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Generator
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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class IntervalSchedule<T> {
    private record ScheduledItem<T>(long interval, T item) {
    }

    private final TreeMap<Long, List<ScheduledItem<T>>> schedule = new TreeMap<>();

    public void schedule(long interval, T item) {
        var firstTime = System.currentTimeMillis();
        var items = schedule.computeIfAbsent(firstTime, ts -> new ArrayList<>());
        items.add(new ScheduledItem<>(interval, item));
    }

    public void schedule(T item) {
        schedule(0, item);
    }

    public T getScheduledItem() {
        var firstScheduled = schedule.firstEntry();
        while (firstScheduled != null) {
            // If the scheduled item is in the future, then return no item
            if (firstScheduled.getKey() > System.currentTimeMillis()) {
                return null;
            }

            if (!firstScheduled.getValue().isEmpty()) {
                // Extract the scheduled item from the list
                var result = firstScheduled.getValue().getFirst();
                firstScheduled.getValue().removeFirst();

                // If not single shot, reschedule for the next interval
                if (result.interval() > 0) {
                    var nextTime = firstScheduled.getKey() + result.interval();
                    var items = schedule.computeIfAbsent(nextTime, ts -> new ArrayList<>());
                    items.add(result);
                }
                // Finally return the scheduled item
                return result.item;
            }
            schedule.remove(firstScheduled.getKey());
            firstScheduled = schedule.firstEntry();
        }
        return null;
    }
}
