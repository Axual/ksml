package io.axual.ksml.util;

public class ExecutionUtil {
    public interface Method<T> {
        T apply();
    }

    public static <T> T tryThis(Method<T> method) {
        try {
            return method.apply();
        } catch (Exception e) {
            // Ignore
        }
        return null;
    }
}
