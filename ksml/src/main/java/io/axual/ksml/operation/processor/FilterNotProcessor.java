package io.axual.ksml.operation.processor;

import io.axual.ksml.store.StateStores;
import org.apache.kafka.streams.processor.api.Record;

public class FilterNotProcessor extends OperationProcessor {
    public interface Predicate {
        boolean test(StateStores stores, Record<Object, Object> record);
    }

    private final Predicate action;

    public FilterNotProcessor(String name, Predicate action, String[] storeNames) {
        super(name, storeNames);
        this.action = action;
    }

    @Override
    public void process(Record<Object, Object> record) {
        if (!action.test(stores, record)) {
            context.forward(record);
        }
    }
}
