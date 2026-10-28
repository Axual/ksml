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
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The result of comparing both sides of one parity case for one optimization setting.
 *
 * @param differences behaviour that is not the same, each entry is one finding
 * @param notes       things worth knowing that do not count as a difference (topology shape, number format)
 */
public record ParityComparison(ParityCase parityCase, String optimization, SideResult reference, SideResult ksml,
                               List<String> differences, List<String> notes) {
    private static final int MAX_RECORD_DIFFS = 12;

    static ParityComparison compare(ParityCase parityCase, String optimization, SideResult ref, SideResult ksml) {
        final var differences = new ArrayList<String>();
        final var notes = new ArrayList<String>();

        final var refError = ref.error();
        final var ksmlError = ksml.error();
        if (refError == null && ksmlError != null) {
            differences.add("KSML " + ksmlError + " (Kafka Streams did not fail)");
        } else if (refError != null && ksmlError == null) {
            differences.add("Kafka Streams " + refError + " (KSML did not fail)");
        } else if (refError != null) {
            // On Docker there is no input step number, so failing on both sides counts as the same
            final var sameStep = ref.processingError != null && ksml.processingError != null
                    && ("docker".equals(optimization) || stepOf(ref.processingError).equals(stepOf(ksml.processingError)));
            final var bothBuild = ref.buildError != null && ksml.buildError != null;
            (sameStep || bothBuild ? notes : differences).add("both failed. Kafka Streams " + refError + " / KSML " + ksmlError);
        }

        if (ref.buildError == null && ksml.buildError == null) {
            compareRecords(parityCase, ref, ksml, differences, notes);
            compareStores(ref, ksml, differences);
            if (!ref.printed.equals(ksml.printed)) {
                differences.add("printed output\n      kafka streams: " + ref.printed + "\n      ksml         : " + ksml.printed);
            }
            compareShape(parityCase, ref, ksml, differences, notes);
            compareChangelogs(ref, ksml, parityCase.compareChangelogs ? differences : notes);
        }
        if (ksml.harnessError != null) differences.addFirst("HARNESS ERROR, the case did not run: " + ksml.harnessError);
        ref.notes.forEach(n -> notes.add("Kafka Streams: " + n));
        ksml.notes.forEach(n -> notes.add("KSML: " + n));
        if (ref.driverError != null) notes.add("Kafka Streams driver error: " + ref.driverError);
        if (ksml.driverError != null) notes.add("KSML driver error: " + ksml.driverError);
        return new ParityComparison(parityCase, optimization, ref, ksml, differences, notes);
    }

    private static String stepOf(String processingError) {
        return processingError.substring(0, processingError.indexOf(':'));
    }

    private static void compareRecords(ParityCase parityCase, SideResult ref, SideResult ksml, List<String> differences, List<String> notes) {
        for (final var topic : parityCase.outputs.keySet()) {
            final var refRecords = parityCase.compareFinalValues ? finalValues(ref.records.getOrDefault(topic, List.of())) : ref.records.getOrDefault(topic, List.of());
            final var ksmlRecords = parityCase.compareFinalValues ? finalValues(ksml.records.getOrDefault(topic, List.of())) : ksml.records.getOrDefault(topic, List.of());
            if (refRecords.size() != ksmlRecords.size()) {
                differences.add("topic " + topic + ": Kafka Streams wrote " + refRecords.size() + " records, KSML wrote " + ksmlRecords.size());
            }
            var shown = 0;
            for (var i = 0; i < Math.max(refRecords.size(), ksmlRecords.size()) && shown < MAX_RECORD_DIFFS; i++) {
                final var r = i < refRecords.size() ? refRecords.get(i) : null;
                final var k = i < ksmlRecords.size() ? ksmlRecords.get(i) : null;
                if (!sameRecord(r, k, parityCase.compareHeaders)) {
                    differences.add("topic " + topic + " record #" + i + "\n      kafka streams: "
                            + (r == null ? "-" : r.render(parityCase.compareHeaders)) + "\n      ksml         : "
                            + (k == null ? "-" : k.render(parityCase.compareHeaders)));
                    shown++;
                } else if (r != null && !Objects.equals(r.rawValue(), k.rawValue())) {
                    notes.add("topic " + topic + " record #" + i + " has the same value, but the text differs: kafka streams `"
                            + r.rawValue() + "`, ksml `" + k.rawValue() + "`");
                }
            }
        }
    }

    private static List<SideResult.Rec> finalValues(List<SideResult.Rec> records) {
        final var last = new TreeMap<String, SideResult.Rec>();
        records.forEach(r -> last.put(r.partition() + "/" + Normalizer.render(r.key()), r));
        return last.values().stream()
                .map(r -> new SideResult.Rec(r.key(), r.value(), 0, r.headers(), r.rawValue(), r.partition()))
                .toList();
    }

    private static boolean sameRecord(SideResult.Rec r, SideResult.Rec k, boolean withHeaders) {
        if (r == null || k == null) return false;
        return Objects.equals(r.key(), k.key()) && Objects.equals(r.value(), k.value()) && r.timestamp() == k.timestamp()
                && r.partition() == k.partition()
                && (!withHeaders || Objects.equals(r.headers(), k.headers()));
    }

    private static void compareStores(SideResult ref, SideResult ksml, List<String> differences) {
        for (final var store : ref.stores.keySet()) {
            final var refEntries = ref.stores.get(store);
            final var ksmlEntries = ksml.stores.get(store);
            if (!Objects.equals(refEntries, ksmlEntries)) {
                differences.add("store " + store + "\n      kafka streams: " + refEntries + "\n      ksml         : " + ksmlEntries);
            }
        }
    }

    private static void compareShape(ParityCase parityCase, SideResult ref, SideResult ksml, List<String> differences, List<String> notes) {
        if (ref.shape == null || ksml.shape == null) return;
        final var sameShape = ref.shape.subTopologies() == ksml.shape.subTopologies()
                && ref.shape.repartitionTopics() == ksml.shape.repartitionTopics()
                && ref.shape.globalStores() == ksml.shape.globalStores()
                && ref.shape.stores().size() == ksml.shape.stores().size();
        if (!sameShape) {
            (parityCase.compareShape ? differences : notes).add("topology shape differs\n      kafka streams: " + ref.shape
                    + "\n      ksml         : " + ksml.shape);
        }
    }

    private static void compareChangelogs(SideResult ref, SideResult ksml, List<String> notes) {
        final var refChangelogs = changelogs(ref);
        final var ksmlChangelogs = changelogs(ksml);
        if (refChangelogs.size() != ksmlChangelogs.size()) {
            notes.add("changelog topics written differ\n      kafka streams: " + refChangelogs + "\n      ksml         : " + ksmlChangelogs);
        }
    }

    private static TreeSet<String> changelogs(SideResult side) {
        final var result = new TreeSet<String>();
        side.producedTopics.stream().filter(t -> t.endsWith("-changelog")).forEach(result::add);
        return result;
    }

    String status() {
        if (ksml.harnessError != null) return "HARNESS-ERROR";
        if (differences.isEmpty()) return "SAME";
        return parityCase.findingId != null ? "KNOWN-DIFF " + parityCase.findingId : "DIFF";
    }

    String details() {
        final var sb = new StringBuilder();
        sb.append(parityCase.id).append(" [optimization=").append(optimization).append("] ").append(parityCase.title)
                .append("\nstatus: ").append(status()).append('\n');
        if (parityCase.findingId != null) {
            sb.append("finding ").append(parityCase.findingId).append(": ").append(parityCase.findingText).append('\n');
        }
        differences.forEach(d -> sb.append("  DIFF ").append(d).append('\n'));
        notes.forEach(n -> sb.append("  NOTE ").append(n).append('\n'));
        return sb.toString();
    }
}
