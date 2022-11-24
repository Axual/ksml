package io.axual.ksml.data.mapper;

import java.util.Comparator;

public class AttributeComparator implements Comparator<String> {
    public static final String META_ATTRIBUTE_CHAR = "@";

    @Override
    public int compare(String o1, String o2) {
        if ((o1 == null || o1.isEmpty()) && (o2 == null || o2.isEmpty())) return 0;
        if (o1 == null || o1.isEmpty()) return -1;
        if (o2 == null || o2.isEmpty()) return 1;

        var meta1 = o1.startsWith(META_ATTRIBUTE_CHAR);
        var meta2 = o2.startsWith(META_ATTRIBUTE_CHAR);

        // If only the first string starts with the meta char, then sort it last
        if (meta1 && !meta2) return 1;
        // If only the second string starts with the meta char, then sort it first
        if (!meta1 && meta2) return -1;
        // If both (do not) start with the meta char, then sort as normal
        return o1.compareTo(o2);
    }
}
