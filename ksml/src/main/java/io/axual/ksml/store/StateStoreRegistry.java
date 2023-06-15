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

<<<<<<<< HEAD:ksml/src/main/java/io/axual/ksml/execution/SerdeWrapper.java
import org.apache.kafka.common.serialization.Serde;

public interface SerdeWrapper<T> {
    Serde<T> wrap(Serde<T> serde);
========
import io.axual.ksml.definition.StateStoreDefinition;

public interface StateStoreRegistry {
    void registerStateStore(String name, StateStoreDefinition store);
>>>>>>>> 1debb8d (Add support for manual state stores, which can in turn be used in Python code):ksml/src/main/java/io/axual/ksml/store/StateStoreRegistry.java
}
