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

import io.axual.ksml.data.object.DataObject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class AlwaysReschedule implements RescheduleStrategy {

    private final List<RescheduleStrategy> others = new ArrayList<>();

    private final Duration interval;

    public AlwaysReschedule(Duration interval) {
        this.interval = interval;
    }

    public void combine(RescheduleStrategy other) {
        others.add(other);
    }

    @Override
    public boolean shouldReschedule(DataObject key, DataObject value) {
        var reschedule = true;
        for (var other: others) {
            reschedule = reschedule && other.shouldReschedule(key, value);
        }
        return reschedule;
    }

    @Override
    public Duration interval() {
        return interval;
    }
}
