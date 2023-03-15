package io.axual.ksml.execution;

public record ErrorHandler(boolean log, boolean logPayload, String loggerName, HandlerType handlerType) {
    public enum HandlerType {
        CONTINUE_ON_FAIL,
        STOP_ON_FAIL
    }
}
