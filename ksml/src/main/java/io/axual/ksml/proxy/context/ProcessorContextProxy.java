package io.axual.ksml.proxy.context;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.data.mapper.HeaderDataObjectMapper;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.definition.DefinitionConstants;
import io.axual.ksml.definition.InjectedParameter;
import io.axual.ksml.proxy.base.AbstractProxy;
import io.axual.ksml.proxy.store.ProxyUtil;
import io.axual.ksml.python.PythonDataObjectMapper;
import io.axual.ksml.python.PythonErrors;
import io.axual.ksml.python.PythonNativeMapper;
import io.axual.ksml.type.UserType;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;

import java.util.Map;
import java.util.TreeSet;

import static io.axual.ksml.type.UserType.DEFAULT_NOTATION;

/**
 * Python view of a Kafka Streams {@link ProcessorContext}, injected into a function as {@code context}. Only the
 * methods annotated with {@link HostAccess.Export} are visible from Python, so for example {@code schedule},
 * {@code getStateStore} and {@code appConfigs} raise an AttributeError. Python code sees the proxy through the class
 * {@code KSMLProcessorContext} of the {@code ksml} module, which adds keyword arguments to {@code forward}.
 * <p>
 * The proxy is only usable until {@link #invalidate()} is called, when the function call that received it returns.
 */
public class ProcessorContextProxy implements AbstractProxy {
    private static final PythonNativeMapper NATIVE_MAPPER = new PythonNativeMapper();
    private static final PythonDataObjectMapper DATA_OBJECT_MAPPER = new PythonDataObjectMapper(true);
    private static final DataObjectConverter CONVERTER = new DataObjectConverter();
    private static final HeaderDataObjectMapper HEADER_MAPPER = new HeaderDataObjectMapper();

    // The class of the ksml Python module that wraps a ProcessorContextProxy
    private static final String PROXY_NAME = "context";
    private static final String PROXY_CLASS_NAME = "KSMLProcessorContext";
    public static final InjectedParameter INJECT_CONTEXT = new InjectedParameter(PROXY_NAME, PROXY_CLASS_NAME);
    private final ProcessorContext<Object, Object> context;
    private final long defaultTimestamp;
    private final Target next;
    private final Map<String, Target> namedTargets;
    private boolean valid = true;
    /**
     * Creates a context proxy.
     *
     * @param context          the Kafka Streams context to delegate to
     * @param defaultTimestamp the timestamp of forwarded records that have none
     * @param next             where {@code forward} without {@code to} sends records
     * @param namedTargets     where {@code forward} with {@code to} sends records, by operation name
     */
    public ProcessorContextProxy(ProcessorContext<Object, Object> context, long defaultTimestamp, Target next, Map<String, Target> namedTargets) {
        this.context = context;
        this.defaultTimestamp = defaultTimestamp;
        this.next = next;
        this.namedTargets = namedTargets;
    }

    private static Object noneToNull(Object object) {
        return object instanceof Value pythonValue && pythonValue.isNull() ? null : object;
    }

    /**
     * Converts a Python key or value to the type the receiving operation takes, as transformer results are.
     */
    private static Object toType(Object object, UserType type, String role, String targetName) {
        DataObject result;
        try {
            result = DATA_OBJECT_MAPPER.toDataObject(type.dataType(), object);
            // As for function results, only a value that does not fit yet gets the notations' converters
            if (type.dataType().isAssignableFrom(result).isNotAssignable())
                result = CONVERTER.convert(DEFAULT_NOTATION, result, type);
        } catch (RuntimeException e) {
            throw PythonErrors.typeError("forward " + role + " for " + targetName + " must be " + type + ", got " + PythonErrors.typeName(object), e);
        }
        if (!(result instanceof DataNull) && type.dataType().isAssignableFrom(result.type()).isNotAssignable())
            throw PythonErrors.typeError("forward " + role + " for " + targetName + " must be " + type + ", got " + result.type());
        // A None key or value must be a real null in Kafka Streams
        return DataObject.nullIfNone(result);
    }

    private static Headers toHeaders(Object headers) {
        if (headers == null)
            return new RecordHeaders();
        try {
            return HEADER_MAPPER.fromDataObject(DATA_OBJECT_MAPPER.toDataObject(DefinitionConstants.HEADERS_TYPE, headers));
        } catch (RuntimeException e) {
            throw PythonErrors.typeError("forward headers must be a list of dicts with a key and a value: " + e.getMessage());
        }
    }

    /**
     * Makes every later call fail, so a context kept by Python after the function call does nothing.
     */
    public void invalidate() {
        valid = false;
    }

    /**
     * Forwards a record. Python {@code None} for {@code timestamp}, {@code headers} or {@code to} means the
     * default timestamp, no headers and the next operation. A {@code None} value is forwarded as a tombstone.
     * The arguments are Python values as they are, so the key and value convert like the result of a function.
     */
    @HostAccess.Export
    public void forward(Value key, Value value, Value timestamp, Value headers, Value to) {
        forward((Object) key, value, timestamp, headers, to);
    }

    /**
     * Like the exported {@code forward}; also takes Java values, and {@code null} for Python {@code None}.
     */
    public void forward(Object key, Object value, Object timestamp, Object headers, Object to) {
        checkValid();
        key = noneToNull(key);
        value = noneToNull(value);
        timestamp = noneToNull(timestamp);
        headers = noneToNull(headers);
        to = noneToNull(to);
        final var name = to != null ? targetName(to) : null;
        final var target = name != null ? namedTargets.get(name) : next;
        final var targetName = name != null ? "operation '" + name + "'" : "the next operation";
        final var rec = new Record<>(
                toType(key, target.keyType(), "key", targetName),
                toType(value, target.valueType(), "value", targetName),
                toTimestamp(timestamp),
                toHeaders(headers));
        if (target.childName() != null)
            context.forward(rec, target.childName());
        else
            context.forward(rec);
    }

    /**
     * Asks Kafka Streams to commit the task at its next opportunity.
     */
    @HostAccess.Export
    public void commit() {
        checkValid();
        context.commit();
    }

    @HostAccess.Export
    public long currentSystemTimeMs() {
        checkValid();
        return context.currentSystemTimeMs();
    }

    @HostAccess.Export
    public long currentStreamTimeMs() {
        checkValid();
        return context.currentStreamTimeMs();
    }

    @HostAccess.Export
    public String applicationId() {
        checkValid();
        return context.applicationId();
    }

    /**
     * The task id, for example {@code "0_2"}.
     */
    @HostAccess.Export
    public String taskId() {
        checkValid();
        return context.taskId().toString();
    }

    private void checkValid() {
        if (!valid)
            throw PythonErrors.runtimeError("The context is no longer valid: it can only be used during the function call that received it");
    }

    /**
     * Returns the name of a forward target, after checking that it is one.
     */
    private String targetName(Object to) {
        if (NATIVE_MAPPER.fromPython(to) instanceof String name) {
            if (namedTargets.containsKey(name))
                return name;
            throw PythonErrors.valueError("forward can not send to '" + name + "', valid names are: "
                    + (namedTargets.isEmpty() ? "none" : String.join(", ", new TreeSet<>(namedTargets.keySet()))));
        }
        throw PythonErrors.typeError("forward to must be an operation name, got " + PythonErrors.typeName(to));
    }

    private long toTimestamp(Object timestamp) {
        if (timestamp == null)
            return defaultTimestamp;
        final Object value;
        try {
            value = NATIVE_MAPPER.fromPython(timestamp);
        } catch (DataException e) {
            // An int that does not fit in a long
            throw PythonErrors.valueError("forward timestamp is out of range", e);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long)
            return ProxyUtil.checkTimestamp(((Number) value).longValue(), "forward");
        throw PythonErrors.typeError("forward timestamp must be an int, got " + PythonErrors.typeName(timestamp));
    }

    /**
     * An operation that {@code forward} can send records to.
     *
     * @param childName the Kafka Streams child name to forward to, or {@code null} to forward to all children
     * @param keyType   the key type the receiving operation takes
     * @param valueType the value type the receiving operation takes
     */
    public record Target(
            String childName,
            UserType keyType,
            UserType valueType) {
    }
}
