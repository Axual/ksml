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

import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.user.UserFunction;

public class UntilReschedule implements RescheduleStrategy {

    private final UserFunction condition;

    public UntilReschedule(UserFunction condition) {
        this.condition = condition;
    }

    @Override
    public boolean shouldReschedule(DataObject key, DataObject value) {
        DataObject result = condition.call(key, value);

        // Note: "until" should NOT reschedule if the predicate became true, negate the result!
        if (result instanceof DataBoolean resultBoolean) return !(resultBoolean.value());
        throw new io.axual.ksml.data.exception.ExecutionException("Producer condition did not return a boolean value: " + condition.name);
    }
}
