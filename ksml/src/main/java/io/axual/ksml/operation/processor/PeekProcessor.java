package io.axual.ksml.operation.processor;

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.api.Record;

public class PeekProcessor extends OperationProcessor {
    public interface PeekAction {
        void apply(StateStores stores, Record<Object, Object> record);
    }

    private final PeekAction action;

    public PeekProcessor(String name, PeekAction action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> record) {
        action.apply(stores, record);
        context.forward(record);
    }
}
