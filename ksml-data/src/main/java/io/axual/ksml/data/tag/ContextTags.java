package io.axual.ksml.data.tag;

import lombok.EqualsAndHashCode;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
public class ContextTags extends ArrayList<ContextTag> {
    public ContextTags append(ContextTag tag) {
        final var result = new ContextTags();
        result.addAll(this);
        result.add(tag);
        return result;
    }

    public ContextTags append(String key, String value) {
        return append(new ContextTag(key, value));
    }

    @Override
    public String toString() {
        final var result = new StringBuilder();
        forEach(t -> result.append(!result.isEmpty() ? "," : "").append(t));
        return result.toString();
    }
}
