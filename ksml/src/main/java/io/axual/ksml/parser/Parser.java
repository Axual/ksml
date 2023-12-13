package io.axual.ksml.parser;

public interface Parser<T> {
    T parse(YamlNode node);
}
