package io.axual.ksml.metric;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import java.util.Collections;

public class Metrics {
    public static final String DOMAIN = "ksml";

    private Metrics() {
    }

    private static final MetricsRegistry REGISTRY = new MetricsRegistry();

    public static MetricsRegistry registry() {
        return REGISTRY;
    }

    public static synchronized void init() {
        registry().enableJmx(DOMAIN, Collections.emptyList());
    }
}
