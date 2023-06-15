package io.axual.ksml.operation.processor;

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.api.Record;

public class TransformKeyProcessor extends OperationProcessor {
    public interface TransformKeyAction {
        Object apply(StateStores stores, Record<Object, Object> record);
    }

    private final TransformKeyAction action;

    public TransformKeyProcessor(String name, TransformKeyAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> record) {
        var k = action.apply(stores, record);
        context.forward(record.withKey(k));
    }
}
