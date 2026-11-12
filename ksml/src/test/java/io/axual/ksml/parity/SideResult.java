package io.axual.ksml.parity;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * What one side (Kafka Streams or KSML) produced for one parity case and one optimization setting.
 */
final class SideResult {
    record Rec(Object key, Object value, long timestamp, Map<String, String> headers, String rawValue, int partition) {
        String render(boolean withHeaders) {
            return (partition >= 0 ? "partition=" + partition + " " : "") + "key=" + Normalizer.render(key)
                    + " value=" + Normalizer.render(value) + " ts=" + timestamp + (withHeaders ? " headers=" + headers : "");
        }
    }

    final String side;
    String buildError;
    String processingError;
    String driverError;
    String harnessError;
    String topologyDescription;
    Shape shape;
    final Map<String, List<Rec>> records = new LinkedHashMap<>();
    final Map<String, List<String>> stores = new LinkedHashMap<>();
    final TreeSet<String> producedTopics = new TreeSet<>();
    String printFile;
    final List<String> printed = new ArrayList<>();
    final List<String> notes = new ArrayList<>();

    SideResult(String side) {
        this.side = side;
    }

    String error() {
        if (buildError != null) return "build failed: " + buildError;
        if (processingError != null) return "processing failed: " + processingError;
        return null;
    }

    /**
     * A short summary of the topology, used to compare the shape of both sides.
     */
    record Shape(int subTopologies, int repartitionTopics, int globalStores, TreeSet<String> stores) {
        @Override
        public String toString() {
            return "subTopologies=" + subTopologies + ", repartitionTopics=" + repartitionTopics
                    + ", globalStores=" + globalStores + ", stores=" + stores;
        }
    }
}
