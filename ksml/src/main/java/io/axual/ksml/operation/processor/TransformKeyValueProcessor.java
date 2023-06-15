package io.axual.ksml.operation.processor;

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Record;

public class TransformKeyValueProcessor extends OperationProcessor {
    public interface TransformKeyValueAction {
        KeyValue<Object, Object> apply(StateStores stores, Record<Object, Object> record);
    }

    private final TransformKeyValueAction action;

    public TransformKeyValueProcessor(String name, TransformKeyValueAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> record) {
        var kv = action.apply(stores, record);
        if (kv != null) {
            context.forward(record.withKey(kv.key).withValue(kv.value));
        }
    }
}
