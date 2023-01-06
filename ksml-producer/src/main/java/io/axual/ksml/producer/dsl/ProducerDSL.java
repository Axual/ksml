package io.axual.ksml.producer.dsl;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


public class ProducerDSL {
    private ProducerDSL() {
    }

    public static final String FUNCTION_TYPE_GENERATOR = "generator";
    public static final String PRODUCERS_DEFINITION = "producers";
    public static final String GENERATOR_ATTRIBUTE = "generator";
    public static final String INTERVAL_ATTRIBUTE = "interval";
    public static final String TARGET_ATTRIBUTE = "to";
}
