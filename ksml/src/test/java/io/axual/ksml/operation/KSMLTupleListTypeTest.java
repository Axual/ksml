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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@ExtendWith(KSMLTestExtension.class)
class KSMLTupleListTypeTest {

    @KSMLTopic(topic = "input_topic")
    TestInputTopic<String, String> inputTopic;

    @KSMLTopic(topic = "output_list_topic")
    TestOutputTopic<String, String> outputListTopic;
    
    @KSMLTopic(topic = "output_tuple_topic")
    TestOutputTopic<String, String> outputTupleTopic;
    
    @KSMLTopic(topic = "output_list_of_tuples_topic")
    TestOutputTopic<String, String> outputListOfTuplesTopic;

    @KSMLTest(topology = "pipelines/test-tuple-list-types.yaml")
    @DisplayName("tuple() function syntax should work for keyValueTransformer")
    void testTupleFunctionSyntax() {
        // Given: Input message
        inputTopic.pipeInput("key1", "value1");
        
        // When: Processing through pipeline with tuple() syntax
        // The pipeline uses tuple_function_syntax which returns tuple(string, string)
        
        // Then: Output should contain transformed key-value pair
        List<KeyValue<String, String>> output = outputTupleTopic.readKeyValuesToList();
        
        assertThat(output)
                .as("Should produce one message with tuple() syntax")
                .hasSize(1);
                
        assertThat(output.getFirst().key)
                .as("Key should be transformed with tuple_ prefix")
                .isEqualTo("tuple_key1");
                
        assertThat(output.getFirst().value)
                .as("Value should be transformed with tuple_ prefix")
                .isEqualTo("tuple_value1");
    }

    @KSMLTest(topology = "pipelines/test-tuple-list-types.yaml")
    @DisplayName("list() syntax should work for keyValueToValueListTransformer")
    void testListFunctionSyntax() {
        // Given: Input message
        inputTopic.pipeInput("test_key", "test_value");
        
        // When: Processing through pipeline with list() syntax
        // The pipeline uses nested_list_tuple which returns list(string)
        
        // Then: Should produce multiple messages (one for each value in the list)
        List<KeyValue<String, String>> output = outputListTopic.readKeyValuesToList();
        
        assertThat(output)
                .as("Should produce 3 messages from list() return type")
                .hasSize(3);
                
        // Verify the values contain the expected pattern
        for (int i = 0; i < 3; i++) {
            assertThat(output.get(i).value)
                    .as("Value should contain key, index, and value")
                    .contains("test_key")
                    .contains(String.valueOf(i))
                    .contains("test_value");
        }
    }

    @KSMLTest(topology = "pipelines/test-tuple-list-types.yaml")
    @DisplayName("list(tuple(keyType,valueType)) complex syntax should work")
    void testListOfTuplesSyntax() {
        // Given: Input message
        inputTopic.pipeInput("complex_key", "complex_value");
        
        // When: Processing through pipeline with list(tuple(string, string)) syntax
        // The pipeline uses list_of_tuples_function which returns list(tuple(string, string))
        
        // Then: Should produce multiple key-value pairs (2 messages from the list)
        List<KeyValue<String, String>> output = outputListOfTuplesTopic.readKeyValuesToList();
        
        assertThat(output)
                .as("Should produce 2 messages from list(tuple()) return type")
                .hasSize(2);
                
        // Verify first tuple
        assertThat(output.get(0).key)
                .as("First tuple key should have tuple_0 suffix")
                .isEqualTo("complex_key_tuple_0");
                
        assertThat(output.get(0).value)
                .as("First tuple value should have modified_0 suffix")
                .isEqualTo("complex_value_modified_0");
                
        // Verify second tuple
        assertThat(output.get(1).key)
                .as("Second tuple key should have tuple_1 suffix")
                .isEqualTo("complex_key_tuple_1");
                
        assertThat(output.get(1).value)
                .as("Second tuple value should have modified_1 suffix")
                .isEqualTo("complex_value_modified_1");
    }
}