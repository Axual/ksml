package io.axual.ksml.definition;

import java.time.Duration;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StoreDefinition {
    public final String name;
    public final Duration retention;
    public final Boolean caching;
}
