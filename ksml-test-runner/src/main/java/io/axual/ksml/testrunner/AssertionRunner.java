package io.axual.ksml.testrunner;

/*-
 * ========================LICENSE_START=================================
 * KSML Test Runner
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

import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.python.PythonTypeConverter;
import io.axual.ksml.store.StateStoreProxyFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes Python assertion code against output records and state stores.
 */
@Slf4j
public class AssertionRunner {

    private final TopologyTestDriver driver;

    public AssertionRunner(TopologyTestDriver driver) {
        this.driver = driver;
    }

    /**
     * Run all assertion blocks and return the result.
     *
     * @param assertBlocks the assertion blocks to execute
     * @param testName     the test name for reporting
     * @return the test result
     */
    public TestResult runAssertions(List<AssertBlock> assertBlocks, String testName) {
        var pythonContext = new PythonContext(PythonContextConfig.builder().build());

        try {
            for (int i = 0; i < assertBlocks.size(); i++) {
                var block = assertBlocks.get(i);
                log.debug("Running assertion block {}/{}", i + 1, assertBlocks.size());

                var result = runSingleAssertion(pythonContext, block, testName);
                if (result.status() != TestResult.Status.PASS) {
                    return result;
                }
            }
            return TestResult.pass(testName);
        } catch (Exception e) {
            log.error("Unexpected error running assertions for test '{}'", testName, e);
            return TestResult.error(testName, e.getMessage());
        }
    }

    private TestResult runSingleAssertion(PythonContext pythonContext, AssertBlock block, String testName) {
        try {
            // Build the Python code that injects variables and runs assertions
            var setupCode = new StringBuilder();
            setupCode.append("import polyglot\n\n");

            // Collect variables to inject
            var variableNames = new ArrayList<String>();
            var variableValues = new ArrayList<>();

            // If topic is specified, collect output records
            if (block.topic() != null) {
                var records = collectOutputRecords(block.topic());
                variableNames.add("records");
                variableValues.add(PythonTypeConverter.toPython(records));
            }

            // If stores are specified, inject store proxies
            if (block.stores() != null) {
                for (var storeName : block.stores()) {
                    var store = driver.getKeyValueStore(storeName);
                    if (store == null) {
                        return TestResult.error(testName,
                                "State store '" + storeName + "' not found in topology");
                    }
                    var proxy = StateStoreProxyFactory.wrap(store);
                    variableNames.add(storeName);
                    variableValues.add(proxy);
                }
            }

            // Inject variables one at a time using prefixed parameter names
            for (int j = 0; j < variableNames.size(); j++) {
                var varName = variableNames.get(j);
                var varValue = variableValues.get(j);
                var code = String.format("""
                        %s = None
                        import polyglot
                        @polyglot.export_value
                        def _set_%s(_val):
                            global %s
                            %s = _val
                        """, varName, varName, varName, varName);
                var setter = pythonContext.registerFunction(code, "_set_" + varName);
                if (setter != null) {
                    setter.execute(varValue);
                }
            }

            // Execute the assertion code
            try {
                pythonContext.registerFunction(
                        "def _run_assertions():\n" +
                                block.code().lines()
                                        .map(line -> "  " + line)
                                        .reduce((a, b) -> a + "\n" + b)
                                        .orElse("  pass") +
                                "\n\nimport polyglot\n@polyglot.export_value\ndef _exec_assertions():\n  _run_assertions()\n",
                        "_exec_assertions"
                ).execute();
            } catch (org.graalvm.polyglot.PolyglotException e) {
                if (e.getMessage() != null && e.getMessage().contains("AssertionError")) {
                    var msg = extractAssertionMessage(e);
                    return TestResult.fail(testName, msg);
                }
                return TestResult.error(testName, e.getMessage());
            }

            return TestResult.pass(testName);
        } catch (Exception e) {
            log.error("Error running assertion for test '{}'", testName, e);
            return TestResult.error(testName, e.getMessage());
        }
    }

    private List<Map<String, Object>> collectOutputRecords(String topic) {
        var outputTopic = driver.createOutputTopic(
                topic, new StringDeserializer(), new StringDeserializer());

        var records = new ArrayList<Map<String, Object>>();
        var keyValues = outputTopic.readRecordsToList();
        for (var record : keyValues) {
            var map = new HashMap<String, Object>();
            map.put("key", record.key());
            map.put("value", record.value());
            map.put("timestamp", record.timestamp());
            records.add(map);
        }
        return records;
    }

    private String extractAssertionMessage(org.graalvm.polyglot.PolyglotException e) {
        var message = e.getMessage();
        if (message != null && message.contains("AssertionError")) {
            // Try to extract the user-provided assertion message
            var idx = message.indexOf("AssertionError:");
            if (idx >= 0) {
                return message.substring(idx);
            }
            idx = message.indexOf("AssertionError");
            if (idx >= 0) {
                return message.substring(idx);
            }
        }
        return message;
    }
}
