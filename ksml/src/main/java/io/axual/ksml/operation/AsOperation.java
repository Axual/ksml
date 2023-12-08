package io.axual.ksml.operation;

import io.axual.ksml.generator.TopologyBuildContext;
import io.axual.ksml.stream.CogroupedKStreamWrapper;
import io.axual.ksml.stream.GlobalKTableWrapper;
import io.axual.ksml.stream.KGroupedStreamWrapper;
import io.axual.ksml.stream.KGroupedTableWrapper;
import io.axual.ksml.stream.KStreamWrapper;
import io.axual.ksml.stream.KTableWrapper;
import io.axual.ksml.stream.SessionWindowedCogroupedKStreamWrapper;
import io.axual.ksml.stream.SessionWindowedKStreamWrapper;
import io.axual.ksml.stream.StreamWrapper;
import io.axual.ksml.stream.TimeWindowedCogroupedKStreamWrapper;
import io.axual.ksml.stream.TimeWindowedKStreamWrapper;

public class AsOperation extends BaseOperation {
    public final String name;

    public AsOperation(OperationConfig config, String name) {
        super(config);
        this.name = name;
    }

    @Override
    public StreamWrapper apply(KStreamWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(KTableWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(GlobalKTableWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(KGroupedStreamWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(KGroupedTableWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(SessionWindowedKStreamWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(TimeWindowedKStreamWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(CogroupedKStreamWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(SessionWindowedCogroupedKStreamWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }

    @Override
    public StreamWrapper apply(TimeWindowedCogroupedKStreamWrapper input, TopologyBuildContext context) {
        context.registerStreamWrapper(name, input);
        return null;
    }
}
