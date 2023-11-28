package io.axual.ksml.generator;

import io.axual.ksml.data.mapper.DataObjectConverter;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.GlobalTableDefinition;
import io.axual.ksml.definition.KeyValueStateStoreDefinition;
import io.axual.ksml.definition.Ref;
import io.axual.ksml.definition.SessionStateStoreDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.definition.StreamDefinition;
import io.axual.ksml.definition.TableDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.WindowStateStoreDefinition;
import io.axual.ksml.exception.KSMLParseException;
import io.axual.ksml.exception.KSMLTopologyException;
import io.axual.ksml.execution.FatalError;
import io.axual.ksml.notation.NotationLibrary;
import io.axual.ksml.python.PythonContext;
import io.axual.ksml.python.PythonFunction;
import io.axual.ksml.store.StoreUtil;
import io.axual.ksml.stream.BaseStreamWrapper;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.user.UserFunction;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class TopologyBuildContext {
    private final StreamsBuilder builder;
    private final TopologySpecification specification;
    private final PythonContext pythonContext;
    private final NotationLibrary notationLibrary;
    private final String namePrefix;

    // The names of all state stores that were created, either through explicit creation or through the use of a
    // Materialized parameter in any of the Kafka Streams operations.
    private final Set<String> createdStateStores = new HashSet<>();

    // All wrapped KStreams, KTables and KGlobalTables
    private final Map<String, StreamWrapper> streamWrappers = new HashMap<>();
    private final Map<String, AtomicInteger> typeInstanceCounters = new HashMap<>();

    public TopologyBuildContext(StreamsBuilder builder, TopologySpecification specification, NotationLibrary notationLibrary, String namePrefix) {
        this.builder = builder;
        this.specification = specification;
        this.notationLibrary = notationLibrary;
        this.namePrefix = namePrefix;
        this.pythonContext = new PythonContext(new DataObjectConverter(notationLibrary));
    }

    private <T> T lookup(Ref<T> ref, Function<String, T> lookup, String description, boolean allowNull) {
        if (ref.definition() != null) return ref.definition();
        if (ref.name() == null) {
            if (allowNull) return null;
            throw new KSMLParseException(ref.referer(), "Missing " + description + " in specification");
        }
        final var result = lookup.apply(ref.name());
        if (result != null) return result;
        throw new KSMLParseException(ref.referer(), "Unknown " + description + " \"" + ref.name() + "\"");
    }

    public FunctionDefinition lookupFunction(Ref<FunctionDefinition> ref, String functionType) {
        return lookup(ref, specification.getFunctionDefinitions()::get, functionType + " function definition", false);
    }

    public <T extends TopicDefinition> T lookupTopic(Ref<T> ref, String streamType) {
        return lookup(ref, ((Map<String, T>) specification.getTopicDefinitions())::get, (streamType != null ? streamType : "topic") + " definition", false);
    }

    public StreamDefinition lookupStream(Ref<StreamDefinition> ref) {
        return lookupTopic(ref, "stream");
    }

    public StateStoreDefinition lookupStore(Ref<StateStoreDefinition> ref) {
        return lookup(ref, specification.getStateStoreDefinitions()::get, "state store", true);
    }

    public void createUserStateStore(StateStoreDefinition store) {
        if (store instanceof KeyValueStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef, notationLibrary);
            builder.addStateStore(storeBuilder);
        }

        if (store instanceof SessionStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef, notationLibrary);
            builder.addStateStore(storeBuilder);
        }

        if (store instanceof WindowStateStoreDefinition storeDef) {
            final var storeBuilder = StoreUtil.getStoreBuilder(storeDef, notationLibrary);
            builder.addStateStore(storeBuilder);
        }

        createdStateStores.add(store.name());
    }

    public Set<String> createdStateStores() {
        return createdStateStores;
    }

    public <T extends BaseStreamWrapper> T getStreamWrapper(TopicDefinition definition, Class<T> resultClass) {
        // We do not know the name of the StreamWrapper here, only its definition (which may be inlined in KSML), so we
        // perform a lookup based on the topic name. If we find it, we return that StreamWrapper. If not, we create it,
        // register it and return it here.
        var result = streamWrappers.get(definition.getTopic());
        if (result == null) {
            result = buildWrapper(definition.getTopic(), definition);
        }
        if (!resultClass.isInstance(result)) {
            throw new KSMLTopologyException("Stream is of incorrect dataType " + result.getClass().getSimpleName() + " where " + resultClass.getSimpleName() + " expected");
        }
        return (T) result;
    }

    public BaseStreamWrapper getStreamWrapper(Ref<? extends TopicDefinition> definition, String type) {
        return getStreamWrapper(lookupTopic(definition, type), BaseStreamWrapper.class);
    }

    private StreamWrapper buildWrapper(String name, TopicDefinition def) {
        if (def instanceof StreamDefinition streamDefinition) {
            var streamKey = new StreamDataType(notationLibrary, streamDefinition.getKeyType(), true);
            var streamValue = new StreamDataType(notationLibrary, streamDefinition.getValueType(), false);
            return new KStreamWrapper(
                    builder.stream(streamDefinition.getTopic(), Consumed.with(streamKey.getSerde(), streamValue.getSerde()).withName(name)),
                    streamKey,
                    streamValue);
        }

        if (def instanceof TableDefinition tableDefinition) {
            final var streamKey = new StreamDataType(notationLibrary, tableDefinition.getKeyType(), true);
            final var streamValue = new StreamDataType(notationLibrary, tableDefinition.getValueType(), false);

            if (tableDefinition.getStore() != null) {
                final var mat = StoreUtil.materialize(tableDefinition.getStore(), notationLibrary);
                return new KTableWrapper(builder.table(tableDefinition.getTopic(), mat), streamKey, streamValue);
            }

            final var consumed = Consumed.as(name).withKeySerde(streamKey.getSerde()).withValueSerde(streamValue.getSerde());
            return new KTableWrapper(builder.table(tableDefinition.getTopic(), consumed), streamKey, streamValue);
        }

        if (def instanceof GlobalTableDefinition globalTableDefinition) {
            final var streamKey = new StreamDataType(notationLibrary, globalTableDefinition.getKeyType(), true);
            final var streamValue = new StreamDataType(notationLibrary, globalTableDefinition.getValueType(), false);
            final var consumed = Consumed.as(name).withKeySerde(streamKey.getSerde()).withValueSerde(streamValue.getSerde());
            return new GlobalKTableWrapper(builder.globalTable(globalTableDefinition.getTopic(), consumed), streamKey, streamValue);
        }

        throw FatalError.topologyError("Unknown stream type: " + def.getClass().getSimpleName());
    }

    public UserFunction createUserFunction(String name, Ref<FunctionDefinition> ref) {
        if (ref.definition() != null) {
            return PythonFunction.fromAnon(pythonContext, name, ref.definition(), ref.referer().getDottedName());
        }
        return PythonFunction.fromNamed(pythonContext, ref.name(), lookupFunction(ref, "function"));
    }

    //
//
//    @Override
//    public String getNamePrefix() {
//        return namePrefix;
//    }
//
//
//    @Override
//    public Map<String, AtomicInteger> getTypeInstanceCounters() {
//        return typeInstanceCounters;
//    }
//
//    @Override
//    public NotationLibrary getNotationLibrary() {
//        return notationLibrary;
//    }
//
//    public Topology build() {
//        // Create all state stores that were defined, but not yet implicitly created (eg. through using Materialized)
//        for (Map.Entry<String, StateStoreDefinition> entry : stateStoreDefinitions.entrySet()) {
//            if (!createdStateStores.contains(entry.getKey())) {
//                createUserStateStore(entry.getValue());
//                createdStateStores.add(entry.getKey());
//            }
//        }
//        return builder.build();
//    }
}
