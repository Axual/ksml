package io.axual.ksml.runner.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Getter;
import lombok.Setter;

@Setter
//@Getter
public class KSMLErrorHandlingConfig {

    private ErrorHandlingConfig consume;
    private ErrorHandlingConfig produce;
    private ErrorHandlingConfig process;


    public ErrorHandlingConfig getConsumerErrorHandlingConfig() {
        if (consume == null) {
            return getDefaultErrorHandlingConfig("ConsumeError");
        }
        if (consume.getLoggerName() == null) {
            consume.setLoggerName("ConsumeError");
        }
        return consume;
    }

    public ErrorHandlingConfig getProducerErrorHandlingConfig() {
        if (produce == null) {
            return getDefaultErrorHandlingConfig("ProduceError");
        }
        if (produce.getLoggerName() == null) {
            produce.setLoggerName("ProduceError");
        }
        return produce;
    }

    public ErrorHandlingConfig getProcessErrorHandlingConfig() {
        if (process == null) {
            return getDefaultErrorHandlingConfig("ProcessError");
        }
        if (process.getLoggerName() == null) {
            process.setLoggerName("ProcessError");
        }
        return process;
    }

    private ErrorHandlingConfig getDefaultErrorHandlingConfig(String logger) {
        var errorHandlingConfig = new ErrorHandlingConfig();
        errorHandlingConfig.setLoggerName(logger);
        return errorHandlingConfig;
    }

    @Setter
    @Getter
    public static class ErrorHandlingConfig {
        private boolean log = true;
        private boolean logPayload = false;
        private String loggerName;
        private Handler handler = Handler.STOP;

        public enum Handler {
            STOP,
            CONTINUE;

            @JsonCreator
            public static Handler forValues(String value) {
                if (value == null) {
                    return null;
                }

                return switch (value) {
                    case "continueOnFail" -> CONTINUE;
                    case "stopOnFail" -> STOP;
                    default -> null;
                };
            }
        }
    }
}
