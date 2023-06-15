package io.axual.ksml.operation.processor;

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.api.Record;

public class TransformKeyValueToValueListProcessor extends OperationProcessor {
    public interface TransformKeyValueToValueListAction {
        Iterable<Object> apply(StateStores stores, Record<Object, Object> record);
    }

    private final TransformKeyValueToValueListAction action;

    public TransformKeyValueToValueListProcessor(String name, TransformKeyValueToValueListAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> record) {
        var vList = action.apply(stores, record);
        if (vList != null) {
            for (var v : vList) {
                context.forward(record.withValue(v));
            }
        }
    }
}
