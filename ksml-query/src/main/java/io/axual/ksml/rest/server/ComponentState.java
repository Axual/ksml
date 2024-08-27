package io.axual.ksml.rest.server;

/*-
 * ========================LICENSE_START=================================
 * KSML Queryable State Store
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

/**
 * Indicates the state a KSML component or subcomponent is in
 */
public enum ComponentState {
    /**
     * The component is not relevant or used
     */
    NOT_APPLICABLE,
    /**
     * The component is still starting
     */
    STARTING,
    /**
     * The component is in a started state, can be considered active
     */
    STARTED,
    /**
     * The component is shutting down
     */
    STOPPING,
    /**
     * The component has stopped gracefully
     */
    STOPPED,
    /**
     * The component is in a failed state
     */
    FAILED
}
