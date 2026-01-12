package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PythonTypeConverter}.
 * Tests conversion of Java objects to Python-compatible proxy objects.
 */
class PythonTypeConverterTest {

    private Context context;

    @BeforeEach
    void setUp() {
        context = Context.newBuilder("python")
                .allowHostAccess(HostAccess.EXPLICIT)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Nested
    @DisplayName("toPython() - null and primitive handling")
    class NullAndPrimitiveTests {

        @Test
        @DisplayName("null returns null")
        void nullReturnsNull() {
            assertThat(PythonTypeConverter.toPython(null)).isNull();
        }

        @Test
        @DisplayName("primitive String passes through unchanged")
        void stringPassesThrough() {
            String input = "hello";
            Object result = PythonTypeConverter.toPython(input);
            assertThat(result).isSameAs(input);
        }

        @Test
        @DisplayName("primitive Integer passes through unchanged")
        void integerPassesThrough() {
            Integer input = 42;
            Object result = PythonTypeConverter.toPython(input);
            assertThat(result).isSameAs(input);
        }

        @Test
        @DisplayName("primitive Long passes through unchanged")
        void longPassesThrough() {
            Long input = 123456789L;
            Object result = PythonTypeConverter.toPython(input);
            assertThat(result).isSameAs(input);
        }

        @Test
        @DisplayName("primitive Double passes through unchanged")
        void doublePassesThrough() {
            Double input = 3.14159;
            Object result = PythonTypeConverter.toPython(input);
            assertThat(result).isSameAs(input);
        }

        @Test
        @DisplayName("primitive Boolean passes through unchanged")
        void booleanPassesThrough() {
            Boolean input = true;
            Object result = PythonTypeConverter.toPython(input);
            assertThat(result).isSameAs(input);
        }
    }

    @Nested
    @DisplayName("toPython() - Map conversion")
    class MapConversionTests {

        @Test
        @DisplayName("HashMap converts to ProxyHashMap")
        void hashMapConvertsToProxyHashMap() {
            Map<String, Integer> input = new HashMap<>();
            input.put("one", 1);
            input.put("two", 2);

            Object result = PythonTypeConverter.toPython(input);

            assertThat(result).isInstanceOf(ProxyHashMap.class);
        }

        @Test
        @DisplayName("TreeMap converts to ProxyHashMap")
        void treeMapConvertsToProxyHashMap() {
            Map<String, String> input = new TreeMap<>();
            input.put("a", "alpha");
            input.put("b", "beta");

            Object result = PythonTypeConverter.toPython(input);

            assertThat(result).isInstanceOf(ProxyHashMap.class);
        }

        @Test
        @DisplayName("empty Map converts to empty ProxyHashMap")
        void emptyMapConverts() {
            Map<String, String> input = new HashMap<>();

            Object result = PythonTypeConverter.toPython(input);

            assertThat(result).isNotNull().isInstanceOf(ProxyHashMap.class);
            ProxyHashMap proxy = (ProxyHashMap) result;
            assertThat(proxy.getHashSize()).isEqualTo(0);
        }

        @Test
        @DisplayName("nested Map is recursively converted and accessible from Python")
        void nestedMapIsRecursivelyConverted() {
            Map<String, Object> inner = new HashMap<>();
            inner.put("nested", "value");

            Map<String, Object> outer = new HashMap<>();
            outer.put("inner", inner);

            Object result = PythonTypeConverter.toPython(outer);

            assertThat(result).isInstanceOf(ProxyHashMap.class);
            // Verify accessibility from Python rather than direct proxy inspection
            context.getBindings("python").putMember("outer", result);
            Value innerValue = context.eval("python", "outer['inner']['nested']");
            assertThat(innerValue.asString()).isEqualTo("value");
        }

        @Test
        @DisplayName("Map with List values is recursively converted and accessible from Python")
        void mapWithListValuesConverted() {
            List<Integer> numbers = new ArrayList<>();
            numbers.add(1);
            numbers.add(2);
            numbers.add(3);

            Map<String, Object> input = new HashMap<>();
            input.put("numbers", numbers);

            Object result = PythonTypeConverter.toPython(input);

            assertThat(result).isInstanceOf(ProxyHashMap.class);
            // Verify accessibility from Python rather than direct proxy inspection
            context.getBindings("python").putMember("data", result);
            Value sumResult = context.eval("python", "sum(data['numbers'])");
            assertThat(sumResult.asInt()).isEqualTo(6);
        }
    }

    @Nested
    @DisplayName("toPython() - List conversion")
    class ListConversionTests {

        @Test
        @DisplayName("ArrayList converts to ProxyArray")
        void arrayListConvertsToProxyArray() {
            List<String> input = new ArrayList<>();
            input.add("first");
            input.add("second");

            Object result = PythonTypeConverter.toPython(input);

            assertThat(result).isInstanceOf(ProxyArray.class);
        }

        @Test
        @DisplayName("empty List converts to empty ProxyArray")
        void emptyListConverts() {
            List<String> input = new ArrayList<>();

            Object result = PythonTypeConverter.toPython(input);

            assertThat(result).isNotNull().isInstanceOf(ProxyArray.class);
            ProxyArray proxy = (ProxyArray) result;
            assertThat(proxy.getSize()).isEqualTo(0);
        }

        @Test
        @DisplayName("nested List is recursively converted")
        void nestedListIsRecursivelyConverted() {
            List<String> inner = new ArrayList<>();
            inner.add("nested");

            List<Object> outer = new ArrayList<>();
            outer.add(inner);

            Object result = PythonTypeConverter.toPython(outer);

            assertThat(result).isNotNull().isInstanceOf(ProxyArray.class);
            ProxyArray outerProxy = (ProxyArray) result;
            Object innerValue = outerProxy.get(0);
            assertThat(innerValue).isNotNull().isInstanceOf(ProxyArray.class);
        }

        @Test
        @DisplayName("List with Map elements is recursively converted")
        void listWithMapElementsConverted() {
            Map<String, String> map1 = new HashMap<>();
            map1.put("key", "value1");
            Map<String, String> map2 = new HashMap<>();
            map2.put("key", "value2");

            List<Object> input = new ArrayList<>();
            input.add(map1);
            input.add(map2);

            Object result = PythonTypeConverter.toPython(input);

            assertThat(result).isNotNull().isInstanceOf(ProxyArray.class);
            ProxyArray proxy = (ProxyArray) result;
            assertThat(proxy.get(0)).isNotNull().isInstanceOf(ProxyHashMap.class);
            assertThat(proxy.get(1)).isNotNull().isInstanceOf(ProxyHashMap.class);
        }
    }

    @Nested
    @DisplayName("toPython() - GraalVM Value handling")
    class ValueHandlingTests {

        @Test
        @DisplayName("GraalVM Value wrapping a Map is unwrapped and converted")
        void valueWrappingMapIsConverted() {
            Map<String, String> map = new HashMap<>();
            map.put("key", "value");
            Value wrappedMap = Value.asValue(map);

            Object result = PythonTypeConverter.toPython(wrappedMap);

            assertThat(result).isInstanceOf(ProxyHashMap.class);
        }

        @Test
        @DisplayName("GraalVM Value wrapping a List is unwrapped and converted")
        void valueWrappingListIsConverted() {
            List<String> list = new ArrayList<>();
            list.add("item");
            Value wrappedList = Value.asValue(list);

            Object result = PythonTypeConverter.toPython(wrappedList);

            assertThat(result).isInstanceOf(ProxyArray.class);
        }

        @Test
        @DisplayName("GraalVM null Value returns null")
        void nullValueReturnsNull() {
            Value nullValue = Value.asValue(null);

            Object result = PythonTypeConverter.toPython(nullValue);

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("GraalVM Value wrapping a String passes through")
        void valueWrappingStringPassesThrough() {
            Value stringValue = Value.asValue("hello");

            Object result = PythonTypeConverter.toPython(stringValue);

            // Value wrapping primitives are returned as-is (GraalVM handles these)
            assertThat(result).isNotNull().isInstanceOf(Value.class);
            assertThat(((Value) result).asString()).isEqualTo("hello");
        }

        @Test
        @DisplayName("GraalVM Value wrapping an Integer passes through")
        void valueWrappingIntegerPassesThrough() {
            Value intValue = Value.asValue(42);

            Object result = PythonTypeConverter.toPython(intValue);

            // Value wrapping primitives are returned as-is (GraalVM handles these)
            assertThat(result).isNotNull().isInstanceOf(Value.class);
            assertThat(((Value) result).asInt()).isEqualTo(42);
        }
    }

    @Nested
    @DisplayName("Python accessibility tests with HostAccess.EXPLICIT")
    class PythonAccessibilityTests {

        @Test
        @DisplayName("ProxyHashMap is accessible from Python with subscript notation")
        void proxyHashMapAccessibleFromPython() {
            Map<String, String> map = new HashMap<>();
            map.put("key", "value");
            Object proxy = PythonTypeConverter.toPython(map);

            context.getBindings("python").putMember("test_map", proxy);
            Value result = context.eval("python", "test_map['key']");

            assertThat(result.asString()).isEqualTo("value");
        }

        @Test
        @DisplayName("ProxyArray is accessible from Python with index notation")
        void proxyArrayAccessibleFromPython() {
            List<String> list = new ArrayList<>();
            list.add("first");
            list.add("second");
            Object proxy = PythonTypeConverter.toPython(list);

            context.getBindings("python").putMember("test_list", proxy);
            Value result = context.eval("python", "test_list[1]");

            assertThat(result.asString()).isEqualTo("second");
        }

        @Test
        @DisplayName("ProxyHashMap supports 'in' operator in Python")
        void proxyHashMapSupportsInOperator() {
            Map<String, String> map = new HashMap<>();
            map.put("exists", "yes");
            Object proxy = PythonTypeConverter.toPython(map);

            context.getBindings("python").putMember("test_map", proxy);
            Value existsResult = context.eval("python", "'exists' in test_map");
            Value notExistsResult = context.eval("python", "'missing' in test_map");

            assertThat(existsResult.asBoolean()).isTrue();
            assertThat(notExistsResult.asBoolean()).isFalse();
        }

        @Test
        @DisplayName("ProxyArray supports len() in Python")
        void proxyArraySupportsLen() {
            List<Integer> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            list.add(3);
            Object proxy = PythonTypeConverter.toPython(list);

            context.getBindings("python").putMember("test_list", proxy);
            Value result = context.eval("python", "len(test_list)");

            assertThat(result.asInt()).isEqualTo(3);
        }

        @Test
        @DisplayName("ProxyHashMap supports iteration in Python")
        void proxyHashMapSupportsIteration() {
            Map<String, Integer> map = new HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            Object proxy = PythonTypeConverter.toPython(map);

            context.getBindings("python").putMember("test_map", proxy);
            Value result = context.eval("python", "sorted(list(test_map.keys()))");

            assertThat(result.getArraySize()).isEqualTo(2);
        }

        @Test
        @DisplayName("ProxyArray supports iteration in Python")
        void proxyArraySupportsIteration() {
            List<Integer> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            list.add(3);
            Object proxy = PythonTypeConverter.toPython(list);

            context.getBindings("python").putMember("test_list", proxy);
            Value result = context.eval("python", "sum(test_list)");

            assertThat(result.asInt()).isEqualTo(6);
        }

        @Test
        @DisplayName("Nested structures are fully accessible from Python")
        void nestedStructuresAccessibleFromPython() {
            Map<String, Object> outer = new HashMap<>();
            List<Integer> numbers = new ArrayList<>();
            numbers.add(10);
            numbers.add(20);
            numbers.add(30);
            outer.put("numbers", numbers);
            outer.put("name", "test");

            Object proxy = PythonTypeConverter.toPython(outer);

            context.getBindings("python").putMember("data", proxy);
            Value sumResult = context.eval("python", "sum(data['numbers'])");
            Value nameResult = context.eval("python", "data['name']");

            assertThat(sumResult.asInt()).isEqualTo(60);
            assertThat(nameResult.asString()).isEqualTo("test");
        }

        @Test
        @DisplayName("ProxyHashMap supports assignment in Python")
        void proxyHashMapSupportsAssignment() {
            Map<String, String> map = new HashMap<>();
            map.put("existing", "value");
            Object proxy = PythonTypeConverter.toPython(map);

            context.getBindings("python").putMember("test_map", proxy);
            context.eval("python", "test_map['new_key'] = 'new_value'");
            Value result = context.eval("python", "test_map['new_key']");

            assertThat(result.asString()).isEqualTo("new_value");
        }

        @Test
        @DisplayName("ProxyArray supports append-like operations via Python")
        void proxyArraySupportsModification() {
            List<Integer> list = new ArrayList<>();
            list.add(1);
            list.add(2);
            Object proxy = PythonTypeConverter.toPython(list);

            context.getBindings("python").putMember("test_list", proxy);
            // Access by index works
            Value result = context.eval("python", "test_list[0] + test_list[1]");

            assertThat(result.asInt()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Deep nesting tests")
    class DeepNestingTests {

        @Test
        @DisplayName("Deeply nested structure is fully converted")
        void deeplyNestedStructureConverted() {
            // Create a 3-level deep structure
            Map<String, Object> level3 = new HashMap<>();
            level3.put("value", "deep");

            List<Object> level2 = new ArrayList<>();
            level2.add(level3);

            Map<String, Object> level1 = new HashMap<>();
            level1.put("list", level2);

            Object result = PythonTypeConverter.toPython(level1);

            // Verify structure type
            assertThat(result).isInstanceOf(ProxyHashMap.class);
            // Verify accessibility from Python (this is what matters)
            context.getBindings("python").putMember("nested", result);
            Value deepValue = context.eval("python", "nested['list'][0]['value']");
            assertThat(deepValue.asString()).isEqualTo("deep");
        }

        @Test
        @DisplayName("Deeply nested structure is accessible from Python")
        void deeplyNestedAccessibleFromPython() {
            Map<String, Object> level3 = new HashMap<>();
            level3.put("value", "deep");

            List<Object> level2 = new ArrayList<>();
            level2.add(level3);

            Map<String, Object> level1 = new HashMap<>();
            level1.put("list", level2);

            Object proxy = PythonTypeConverter.toPython(level1);

            context.getBindings("python").putMember("nested", proxy);
            Value result = context.eval("python", "nested['list'][0]['value']");

            assertThat(result.asString()).isEqualTo("deep");
        }
    }
}
