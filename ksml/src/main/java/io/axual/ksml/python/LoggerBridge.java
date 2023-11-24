package io.axual.ksml.python;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerBridge {
    public Logger getLogger(String loggerName) {
        return LoggerFactory.getLogger(loggerName);
    }
}
