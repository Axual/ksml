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

/**
 * Fast, isolated unit tests for {@code BaseOperation} subclasses.
 *
 * <p>KSML operations are tested at two deliberately separate layers; add new coverage to the layer
 * that fits what you are checking:
 *
 * <ul>
 *   <li><b>Unit tests (this package)</b> exercise a single operation's {@code apply(...)} with mocked
 *       Kafka Streams inputs and a mocked {@code TopologyBuildContext} (see {@code OperationTestSupport}).
 *       They run fast and on any JVM, without a running Kafka Streams topology or the GraalVM Python
 *       runtime. Use them for things the topology layer cannot easily reach or localize: key/value type
 *       deduction, state-store materialization wiring, error paths, parser edge cases, and confirming an
 *       operation delegates to the correct Streams method (e.g. {@code join} vs {@code leftJoin}).</li>
 *   <li><b>Topology tests</b> ({@code @KSMLTest}, e.g. {@code KSMLFilterTest}, {@code KSMLJoinTest})
 *       run a real Kafka Streams topology from a YAML pipeline and assert the actual output records.
 *       They own end-to-end behavior — "does operation X actually produce Y".</li>
 * </ul>
 *
 * <p>For "does this operation behave correctly", extend the topology layer. The wiring assertions here
 * are the fast first line of defence and intentionally do not re-prove behavior the topology tests own.
 */
package io.axual.ksml.operation;
