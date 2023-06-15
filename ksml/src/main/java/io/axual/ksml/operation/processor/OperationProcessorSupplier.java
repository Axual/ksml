package io.axual.ksml.operation.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class OperationProcessorSupplier<T> implements ProcessorSupplier<Object, Object, Object, Object> {
    public interface ProcessorFactory<T> {
        Processor<Object, Object, Object, Object> create(String name, T action, String[] storeNames);
    }

    protected final String name;
    protected final ProcessorFactory<T> factory;
    protected final T action;
    protected final String[] storeNames;

    public OperationProcessorSupplier(String name, ProcessorFactory<T> factory, T action, String[] storeNames) {
        this.name = name;
        this.factory = factory;
        this.action = action;
        this.storeNames = storeNames;
    }

    @Override
    public Processor<Object, Object, Object, Object> get() {
        return factory.create(name, action, storeNames);
    }
}
