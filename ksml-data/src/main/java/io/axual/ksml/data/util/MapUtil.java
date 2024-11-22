package io.axual.ksml.data.util;

import java.util.HashMap;
import java.util.Map;

public class MapUtil {
    public static Map<String, Object> stringKeys(Map<?, ?> map) {
        final var result = new HashMap<String, Object>();
        map.forEach((key, value) -> result.put(key != null ? key.toString() : null, value));
        return result;
    }
}