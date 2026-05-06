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

import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.parser.UserTypeParser;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonContextConfig;
import io.axual.ksml.proxy.store.ProxyUtil;
import io.axual.ksml.util.Pair;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes Python assertion code against output records and state stores.
 * The runner is given the suite's stream map (keyed by logical stream name);
 * assertion blocks reference output streams via {@code on:} which resolves
 * against this map to the topic + serdes.
 */
@Slf4j
public class AssertionRunner {

    private final TopologyTestDriver driver;
    private final Map<String, StreamDefinition> streams;

    public AssertionRunner(TopologyTestDriver driver, Map<String, StreamDefinition> streams) {
        this.driver = driver;
        this.streams = streams;
    }

    /**
     * Run all assertion blocks and return the result.
     *
     * @param assertBlocks the assertion blocks to execute
     * @param suiteName    the suite-level name for reporting
     * @param testName     the test display label for reporting
     * @return the test result
     */
    public TestResult runAssertions(List<AssertBlock> assertBlocks, String suiteName, String testName) {
        var pythonContext = new PythonContext(PythonContextConfig.builder().build());

        try {
            for (int i = 0; i < assertBlocks.size(); i++) {
                var block = assertBlocks.get(i);
                log.debug("Running assertion block {}/{}", i + 1, assertBlocks.size());

                var result = runSingleAssertion(pythonContext, block, suiteName, testName);
                if (result.status() != TestResult.Status.PASS) {
                    return result;
                }
            }
            return TestResult.pass(suiteName, testName);
        } catch (Exception e) {
            // Log at debug — the exception is preserved in the returned TestResult.message
            log.debug("Unexpected error running assertions for test '{} › {}'", suiteName, testName, e);
            return TestResult.error(suiteName, testName, e.getMessage());
        }
    }

    private TestResult runSingleAssertion(PythonContext pythonContext, AssertBlock block,
                                          String suiteName, String testName) {
        try {
            // Collect variables to inject
            var args = new ArrayList<Pair<String, Object>>();

            // If 'on:' is specified, collect output records from the referenced stream's topic
            if (block.on() != null) {
                var stream = streams.get(block.on());
                if (stream == null) {
                    return TestResult.error(suiteName, testName,
                            "Assert block references undeclared stream '" + block.on() + "'");
                }
                var records = collectOutputRecords(stream);
                args.add(Pair.of("records", ProxyUtil.toPython(records)));
            }

            // If stores are specified, inject store proxies
            if (block.stores() != null) {
                for (var storeName : block.stores()) {
                    var store = driver.getKeyValueStore(storeName);
                    if (store == null) {
                        return TestResult.error(suiteName, testName,
                                "State store '" + storeName + "' not found in topology");
                    }
                    var proxy = ProxyUtil.wrapStateStore(store);
                    args.add(Pair.of(storeName, proxy));
                }
            }

            // Inject variables one at a time using prefixed parameter names
            for (var nameValue : args) {
                var code = """
                        VARNAME = None
                        import polyglot
                        @polyglot.export_value
                        def _set_VARNAME(_val):
                            global VARNAME
                            VARNAME = _val
                        """.replace("VARNAME", nameValue.left());
                var setter = pythonContext.registerFunction(code, "_set_" + nameValue.left());
                if (setter != null) {
                    setter.execute(nameValue.right());
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
                    return TestResult.fail(suiteName, testName, msg);
                }
                return TestResult.error(suiteName, testName, e.getMessage());
            }

            return TestResult.pass(suiteName, testName);
        } catch (Exception e) {
            // Log at debug — the exception is preserved in the returned TestResult.message
            log.debug("Error running assertion for test '{} › {}'", suiteName, testName, e);
            return TestResult.error(suiteName, testName, e.getMessage());
        }
    }

    private static final NativeDataObjectMapper NATIVE_MAPPER = new NativeDataObjectMapper();

    private List<Map<String, Object>> collectOutputRecords(StreamDefinition stream) {
        var keyDeserializer = resolveDeserializer(stream.keyType(), true);
        var valueDeserializer = resolveDeserializer(stream.valueType(), false);

        var outputTopic = driver.createOutputTopic(stream.topic(), keyDeserializer, valueDeserializer);

        var records = new ArrayList<Map<String, Object>>();
        var keyValues = outputTopic.readRecordsToList();
        for (var record : keyValues) {
            var map = new HashMap<String, Object>();
            map.put("key", toNativeValue(record.key()));
            map.put("value", toNativeValue(record.value()));
            map.put("timestamp", record.timestamp());
            records.add(map);
        }
        return records;
    }

    /**
     * Resolve a deserializer for a topic's key or value based on the stream's declared type string.
     * Falls back to StringDeserializer when the type is null or "string".
     */
    @SuppressWarnings("unchecked")
    private Deserializer<Object> resolveDeserializer(String typeString, boolean isKey) {
        if (typeString == null || typeString.equalsIgnoreCase("string")) {
            var stringDeserializer = new StringDeserializer();
            return (Deserializer<Object>) (Deserializer<?>) stringDeserializer;
        }

        try {
            var parsed = new UserTypeParser().parse(typeString);
            if (parsed.isError()) {
                log.warn("Cannot resolve type '{}', falling back to StringDeserializer: {}",
                        typeString, parsed.errorMessage());
                var stringDeserializer = new StringDeserializer();
                return (Deserializer<Object>) (Deserializer<?>) stringDeserializer;
            }
            var streamDataType = new StreamDataType(parsed.result(), isKey);
            return streamDataType.serde().deserializer();
        } catch (Exception e) {
            log.warn("Failed to create deserializer for type '{}', falling back to StringDeserializer",
                    typeString, e);
            var stringDeserializer = new StringDeserializer();
            return (Deserializer<Object>) (Deserializer<?>) stringDeserializer;
        }
    }

    /**
     * Convert a deserialized value to a native Java object suitable for Python injection.
     * DataObjects are converted to native maps/lists, other values pass through.
     */
    private static Object toNativeValue(Object value) {
        if (value instanceof DataObject dataObject) {
            return NATIVE_MAPPER.fromDataObject(dataObject);
        }
        return value;
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
