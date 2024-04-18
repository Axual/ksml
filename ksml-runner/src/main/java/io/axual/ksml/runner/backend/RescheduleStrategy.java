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

import io.axual.ksml.user.UserFunction;

import java.time.Duration;

/**
 * Interface to indicate if an {@link ExecutableProducer} should be rescheduled.
 */
public interface RescheduleStrategy {

    /**
     * Implementations indicate if the producer should be scheduled again.
     * @return
     */
    boolean shouldReschedule();

    /**
     * The only implementation keeping track of interval is {@link AlwaysReschedule}; all others just return {@link Duration#ZERO}.
     * @return the interval until the next reschedule.
     */
    default Duration interval() {
        return Duration.ZERO;
    }

    static AlwaysReschedule always(Duration interval) {
        return new AlwaysReschedule(interval);
    }

    static RescheduleStrategy once() {
        return new SingleShotReschedule();
    }

    static RescheduleStrategy counting(int count) {
        return new CountingReschedule(count);
    }

    static RescheduleStrategy until(UserFunction userFunction) {
        return new UntilReschedule(userFunction);
    }

}
