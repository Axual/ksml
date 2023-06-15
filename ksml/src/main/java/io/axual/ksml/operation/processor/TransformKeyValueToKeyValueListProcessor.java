package io.axual.ksml.operation.processor;

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Record;

public class TransformKeyValueToKeyValueListProcessor extends OperationProcessor {
    public interface TransformKeyValueToKeyValueListAction {
        Iterable<KeyValue<Object, Object>> apply(StateStores stores, Record<Object, Object> record);
    }

    private final TransformKeyValueToKeyValueListAction action;

    public TransformKeyValueToKeyValueListProcessor(String name, TransformKeyValueToKeyValueListAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> record) {
        var kvList = action.apply(stores, record);
        if (kvList != null) {
            for (var kv : kvList) {
                context.forward(record.withKey(kv.key).withValue(kv.value));
            }
        }
    }
}
