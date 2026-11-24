package io.axual.ksml.operation;

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
import io.axual.ksml.testutil.KSMLDriver;
import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Window stores that Python functions use are timestamped stores, unless they set {@code timestamped: false}
 * (AXPD-12445). Python then gets the *WithTimestamp methods, which return the record timestamp.
 */
@ExtendWith(KSMLTestExtension.class)
@SuppressWarnings("java:S2187")
public class KSMLWindowStoreTimestampsTest {

    @KSMLTopic(topic = "window_timestamps_input")
    TestInputTopic<String, String> input;

    @KSMLTopic(topic = "window_timestamps_output")
    TestOutputTopic<String, String> output;

    @KSMLDriver
    TopologyTestDriver driver;

    @KSMLTest(topology = "pipelines/test-window-store-timestamps-python.yaml")
    @DisplayName("Window stores used by Python are timestamped unless they set timestamped: false")
    void testWindowStoreTimestamps() {
        input.pipeInput("a", "x", 1000L);
        input.pipeInput("b", "y", 2000L);

        assertThat(output.readValuesToList()).containsExactly(
                "default=x@1000 memory=x@1000 plain=noTimestamps:x",
                "default=y@2000 memory=y@2000 plain=noTimestamps:y");

        assertThat(driver.getAllStateStores().get("window_default")).isInstanceOf(TimestampedWindowStore.class);
        assertThat(driver.getAllStateStores().get("window_memory")).isInstanceOf(TimestampedWindowStore.class);
        assertThat(driver.getAllStateStores().get("window_plain")).isNotInstanceOf(TimestampedWindowStore.class);
    }
}
