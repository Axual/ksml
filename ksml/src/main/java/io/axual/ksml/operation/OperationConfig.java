package io.axual.ksml.operation;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class OperationConfig {
    private final String name;
    private final boolean allowStores;
    private final String[] storeNames;

    public OperationConfig(String namespace, String name, String[] storeNames) {
        if (namespace != null || name != null) {
            this.name = (namespace != null ? namespace : "")
                    + (namespace != null && name != null ? "_" : "")
                    + (name != null ? name : "");
        } else {
            this.name = null;
        }
        log.debug("Generated operation name: {}", this.name);
        this.allowStores = storeNames != null;
        this.storeNames = storeNames;
    }
}
