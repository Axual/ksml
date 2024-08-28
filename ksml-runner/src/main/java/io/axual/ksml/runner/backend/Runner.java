package io.axual.ksml.runner.backend;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
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


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public interface Runner extends Runnable {
    enum State {
        CREATED(1, 2, 5), // Ordinal 0
        STARTING(2, 3, 4, 5), // Ordinal 1
        STARTED(1, 3, 4, 5), // Ordinal 2
        STOPPING(4, 5), // Ordinal 3
        STOPPED, // Ordinal 4
        FAILED // Ordinal 5
        ;

        State(final Integer... validNextStates) {
            this.validNextStates.addAll(Arrays.asList(validNextStates));
        }

        private final Set<Integer> validNextStates = new HashSet<>();

        public boolean isValidNextState(State nextState) {
            return validNextStates.contains(nextState.ordinal());
        }
    }

    State getState();

    default boolean isRunning() {
        final var state = getState();
        return state == State.STARTING || state == State.STARTED || state == State.STOPPING;
    }

    void stop();
}
