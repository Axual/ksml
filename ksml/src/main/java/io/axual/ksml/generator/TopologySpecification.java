package io.axual.ksml.generator;

import io.axual.ksml.definition.PipelineDefinition;
import io.axual.ksml.definition.TopicDefinition;
import io.axual.ksml.definition.FunctionDefinition;
import io.axual.ksml.definition.StateStoreDefinition;
import io.axual.ksml.exception.KSMLTopologyException;

import java.util.HashMap;
import java.util.Map;

public class TopologySpecification {
    // All registered user functions
    private final Map<String, FunctionDefinition> functionDefinitions = new HashMap<>();
    // All registered pipelines
    private final Map<String, PipelineDefinition> pipelineDefinitions = new HashMap<>();
    // All registered state stores
    private final Map<String, StateStoreDefinition> stateStoreDefinitions = new HashMap<>();
    // All registered KStreams, KTables and KGlobalTables
    private final Map<String, TopicDefinition> topicDefinitions = new HashMap<>();

    public void registerFunction(String name, FunctionDefinition functionDefinition) {
        if (functionDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Function definition must be unique: " + name);
        }
        functionDefinitions.put(name, functionDefinition);
    }

    public Map<String, FunctionDefinition> getFunctionDefinitions() {
        return functionDefinitions;
    }

    public void registerPipeline(String name, PipelineDefinition pipelineDefinition) {
        if (pipelineDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Pipeline definition must be unique: " + name);
        }
        pipelineDefinitions.put(name, pipelineDefinition);
    }

    public Map<String, PipelineDefinition> getPipelineDefinitions() {
        return pipelineDefinitions;
    }

    public void registerStateStore(String name, StateStoreDefinition store) {
        registerStateStore(name, store, false);
    }

    public void registerStateStore(String name, StateStoreDefinition store, boolean canBeDuplicate) {
        // Check if the store is properly named
        if (store.name() == null || store.name().isEmpty()) {
            throw new KSMLTopologyException("StateStore does not have a name: " + store);
        }
        // Check if the name is equal to the store name (otherwise a parsing error occurred)
        if (!store.name().equals(name)) {
            throw new KSMLTopologyException("StateStore name mistmatch: this is a parsing error: " + store);
        }
        // State stores can only be registered once. Duplicate names are a sign of faulty KSML definitions
        if (stateStoreDefinitions.containsKey(store.name())) {
            if (!canBeDuplicate) {
                throw new KSMLTopologyException("StateStore is not unique: " + store.name());
            }
            if (!stateStoreDefinitions.get(store.name()).equals(store)) {
                throw new KSMLTopologyException("StateStore definition conflicts earlier registration: " + store);
            }
        } else {
            stateStoreDefinitions.put(store.name(), store);
        }
    }

    public Map<String, StateStoreDefinition> getStateStoreDefinitions() {
        return stateStoreDefinitions;
    }

    public void registerTopicDefinition(String name, TopicDefinition def) {
        if (topicDefinitions.containsKey(name)) {
            throw new KSMLTopologyException("Topic definition must be unique: " + name);
        }
        topicDefinitions.put(name, def);
    }

    public Map<String, TopicDefinition> getTopicDefinitions() {
        return topicDefinitions;
    }
}
