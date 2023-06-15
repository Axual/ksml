package io.axual.ksml.operation.processor;

import io.axual.ksml.execution.FatalError;
import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;

public abstract class OperationProcessor implements Processor<Object, Object, Object, Object> {
    protected ProcessorContext<Object, Object> context;
    private final String name;
    private final String[] storeNames;
    protected final StateStores stores = new StateStores();

    public OperationProcessor(String name, String[] storeNames) {
        this.name = name;
        this.storeNames = storeNames;
    }

    @Override
    public void init(ProcessorContext<Object, Object> context) {
        this.context = context;
        stores.clear();
        for (String storeName : storeNames) {
            StateStore store = context.getStateStore(storeName);
            if (store == null) {
                throw FatalError.executionError("Could not connect processor '" + name + "' to state store '" + storeName + "'");
            }
            stores.put(storeName, store);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
