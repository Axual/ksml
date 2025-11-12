package io.axual.ksml.operation;

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

import io.axual.ksml.testutil.KSMLTest;
import io.axual.ksml.testutil.KSMLTestExtension;
import io.axual.ksml.testutil.KSMLTopic;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KSMLTestExtension.class)
public class KSMLRepartitionTest {

    @KSMLTopic(topic = "user_activities")
    TestInputTopic<String, String> activitiesInput;

    @KSMLTopic(topic = "user_activity_counts")
    TestOutputTopic<String, String> countsOutput;

    @KSMLTest(topology = "pipelines/test-repartition.yaml")
    void testRepartitionWithOnlyNumberOfPartitions() {
        // This test validates that issue #288 is fixed:
        // Repartition should work with only numberOfPartitions parameter
        // without requiring a custom partitioner function
        // Create test data with region-based keys (uneven distribution)
        String usEastActivity1 = "{\"activity_id\":\"act_00001\",\"user_id\":\"user_001\",\"region\":\"us-east\",\"activity\":\"login\",\"amount\":0,\"timestamp\":1}";
        String usEastActivity2 = "{\"activity_id\":\"act_00002\",\"user_id\":\"user_001\",\"region\":\"us-east\",\"activity\":\"browse\",\"amount\":0,\"timestamp\":2}";
        String usWestActivity = "{\"activity_id\":\"act_00003\",\"user_id\":\"user_002\",\"region\":\"us-west\",\"activity\":\"purchase\",\"amount\":199.99,\"timestamp\":3}";
        String euActivity = "{\"activity_id\":\"act_00004\",\"user_id\":\"user_003\",\"region\":\"eu-central\",\"activity\":\"login\",\"amount\":0,\"timestamp\":4}";
        String apActivity = "{\"activity_id\":\"act_00005\",\"user_id\":\"user_004\",\"region\":\"ap-south\",\"activity\":\"browse\",\"amount\":0,\"timestamp\":5}";
        String usEastActivity3 = "{\"activity_id\":\"act_00006\",\"user_id\":\"user_001\",\"region\":\"us-east\",\"activity\":\"logout\",\"amount\":0,\"timestamp\":6}";

        // Send activities with region as key
        activitiesInput.pipeInput("us-east", usEastActivity1);
        activitiesInput.pipeInput("us-east", usEastActivity2);
        activitiesInput.pipeInput("us-west", usWestActivity);
        activitiesInput.pipeInput("eu-central", euActivity);
        activitiesInput.pipeInput("ap-south", apActivity);
        activitiesInput.pipeInput("us-east", usEastActivity3);

        // Verify output - should have counts by user_id (after repartitioning)
        assertFalse(countsOutput.isEmpty());
        
        // Read all output records
        var outputRecords = countsOutput.readKeyValuesToMap();
        
        // Verify we have counts for each unique user
        assertTrue(outputRecords.containsKey("user_001"));
        assertTrue(outputRecords.containsKey("user_002"));
        assertTrue(outputRecords.containsKey("user_003"));
        assertTrue(outputRecords.containsKey("user_004"));
        
        // Verify counts (handle count formatting)
        assertEquals("3", outputRecords.get("user_001").trim()); // 3 activities for user_001
        assertEquals("1", outputRecords.get("user_002").trim()); // 1 activity for user_002
        assertEquals("1", outputRecords.get("user_003").trim()); // 1 activity for user_003
        assertEquals("1", outputRecords.get("user_004").trim()); // 1 activity for user_004
        
        // The fact that this test completes successfully validates that:
        // 1. Repartition operation works with only numberOfPartitions parameter
        // 2. No "Partitioner must be defined" error occurs
        // 3. The repartitioning successfully redistributes data across 4 partitions
        // 4. Subsequent aggregation operations work correctly after repartitioning
    }
}