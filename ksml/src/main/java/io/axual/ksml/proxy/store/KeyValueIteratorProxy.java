package io.axual.ksml.proxy.store;

import io.axual.ksml.proxy.base.AbstractProxy;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.graalvm.polyglot.HostAccess;

public class KeyValueIteratorProxy implements AbstractProxy {
    private final KeyValueIterator<?, ?> iterator;
    private boolean closed = false;

    public KeyValueIteratorProxy(KeyValueIterator<?, ?> iterator) {
        this.iterator = iterator;
    }

    @HostAccess.Export
    public void close() {
        if (!closed) iterator.close();
        closed = true;
    }

    @HostAccess.Export
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @HostAccess.Export
    public Object next() {
        if (!iterator.hasNext()) return null;
        return ProxyUtil.resultFrom(iterator.next());
    }
}
