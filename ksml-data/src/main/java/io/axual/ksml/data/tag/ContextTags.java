package io.axual.ksml.data.tag;

import java.util.ArrayList;

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
}
