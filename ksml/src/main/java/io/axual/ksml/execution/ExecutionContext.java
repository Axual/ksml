package io.axual.ksml.execution;

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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;

@Slf4j
@Getter
public class ExecutionContext {
    public static final ExecutionContext INSTANCE = new ExecutionContext();
    private final ErrorHandling errorHandling = new ErrorHandling();
    private final NotationLibrary notationLibrary = new NotationLibrary();
    private final SchemaLibrary schemaLibrary = new SchemaLibrary();

    @Getter
    @Setter
    private SerdeWrapper<Object> serdeWrapper = null;

    public Serde<Object> wrapSerde(Serde<Object> serde) {
        return serdeWrapper != null ? serdeWrapper.wrap(serde) : serde;
    }
}
