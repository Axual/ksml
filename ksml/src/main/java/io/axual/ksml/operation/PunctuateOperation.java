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

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.operation.processor.OperationProcessorSupplier;
import io.axual.ksml.operation.processor.PunctuateProcessor;
import io.axual.ksml.proxy.context.ProcessorContextProxy;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;
import io.axual.ksml.user.UserPunctuator;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.PunctuationType;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static io.axual.ksml.proxy.context.ProcessorContextProxy.INJECT_CONTEXT;

/**
 * Calls a punctuator on a schedule; records pass through unchanged. The punctuator can forward records to the next
 * operation, or to a named operation further down the same pipeline: {@link OperationChain} asks for a side path
 * to each of those with {@link #sidePathTo}.
 */
public class PunctuateOperation extends BaseOperation {
    private static final String PUNCTUATOR_NAME = "Punctuator";
    private final Duration interval;
    private final PunctuationType punctuationType;
    private final FunctionDefinition punctuator;
    private final List<String> storeNames;
    // Set by OperationChain before apply: whether a later operation in the pipeline has a name
    private boolean namedOperationsFollow = false;
    // The output of the last apply, and the named targets found for it while the rest of the pipeline is built
    private KStream<Object, Object> output;
    private Map<String, ProcessorContextProxy.Target> namedTargets;

    public PunctuateOperation(OperationConfig config, Duration interval, PunctuationType punctuationType, FunctionDefinition punctuator, List<String> storeNames) {
        super(config);
        this.interval = interval;
        this.punctuationType = punctuationType;
        this.punctuator = punctuator;
        this.storeNames = storeNames != null ? storeNames : List.of();
    }

    /** Sets whether a later operation in the pipeline has a name, and so may become a target of a side path. */
    void namedOperationsFollow(boolean namedOperationsFollow) {
        this.namedOperationsFollow = namedOperationsFollow;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        /*    Kafka Streams method signature:
         *    <KOut, VOut> KStream<KOut, VOut> process(
         *          final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
         *          final Named named,
         *          final String... stateStoreNames)
         */

        checkNotNull(punctuator, PUNCTUATOR_NAME.toLowerCase());
        final var k = input.keyType().flatten();
        final var v = input.valueType().flatten();
        final var allStoreNames = new LinkedHashSet<>(punctuator.storeNames());
        allStoreNames.addAll(storeNames);
        final var function = punctuator.withInjectedParameter(INJECT_CONTEXT).withStoreNames(List.copyOf(allStoreNames));
        final var userFunction = userFunctionOf(context, PUNCTUATOR_NAME, function, DataType.UNKNOWN, superOf(DataLong.DATATYPE));
        final var userPunctuator = new UserPunctuator(userFunction, tags);
        final var stores = allStoreNames.toArray(String[]::new);

        // Input records and records forwarded without a name only go to the next operation. When side paths may
        // follow, the next operation gets a pass-through node with a known name, so the processor can address it.
        final var nextName = namedOperationsFollow ? name + "-next" : null;
        namedTargets = new HashMap<>();
        final var schedule = new PunctuateProcessor.Schedule(interval, punctuationType,
                new ProcessorContextProxy.Target(nextName, k.userType(), v.userType()), namedTargets);
        final var supplier = new OperationProcessorSupplier<PunctuateProcessor.PunctuateAction>(
                name,
                (n, action, s) -> new PunctuateProcessor(n, schedule, action, s),
                userPunctuator::apply,
                stores,
                storeDefinitions(context, stores));
        final var named = namedOf();
        output = named != null
                ? input.stream.process(supplier, named, stores)
                : input.stream.process(supplier, stores);
        final var next = nextName != null ? output.peek((key, value) -> {
        }, Named.as(nextName)) : output;
        return new KStreamWrapper(next, k, v);
    }

    /**
     * Makes a named operation further down the pipeline a target of {@code forward(..., to=...)}: adds a
     * pass-through node on the output of this operation and merges it into the stream right before the target.
     *
     * @param target the operation, which has a name
     * @param input  the stream that the target will be applied to
     * @return the stream to apply the target to
     */
    KStreamWrapper sidePathTo(BaseOperation target, KStreamWrapper input) {
        final var sideName = name + "-to-" + target.name;
        final var side = output.peek((key, value) -> {
        }, Named.as(sideName));
        namedTargets.putIfAbsent(target.definedName(), new ProcessorContextProxy.Target(sideName, input.keyType().flatten().userType(), input.valueType().flatten().userType()));
        return new KStreamWrapper(input.stream.merge(side, Named.as(sideName + "-merge")), input.keyType(), input.valueType());
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        throw needsStream(input);
    }

    @Override
    public StreamWrapper apply(GlobalKTableWrapper input, TopologyBuildContext context) {
        throw needsStream(input);
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input, TopologyBuildContext context) {
        throw needsStream(input);
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input, TopologyBuildContext context) {
        throw needsStream(input);
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input, TopologyBuildContext context) {
        throw needsStream(input);
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input, TopologyBuildContext context) {
        throw needsStream(input);
    }

    private RuntimeException needsStream(StreamWrapper input) {
        return topologyError("Punctuate needs a stream as input, but got " + input + "; use toStream first for a table");
    }
}
