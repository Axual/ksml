package io.axual.ksml.proxy.store;

import io.axual.ksml.proxy.base.AbstractProxy;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class WindowStoreIteratorProxy extends KeyValueIteratorProxy implements AbstractProxy {
    public WindowStoreIteratorProxy(WindowStoreIterator<?> iterator) {
        super(iterator);
    }
}
