package io.axual.ksml.operation.processor;

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.api.Record;

public class TransformValueProcessor extends OperationProcessor {
    public interface TransformValueAction {
        Object apply(StateStores stores, Record<Object, Object> record);
    }

    private final TransformValueAction action;

    public TransformValueProcessor(String name, TransformValueAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> record) {
        var v = action.apply(stores, record);
        context.forward(record.withValue(v));
    }
}
