package io.axual.ksml.runner.logging;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class MockAppender extends AppenderBase<ILoggingEvent> {

    public static final Multimap<String, MockAppender> APPENDERS = HashMultimap.create();

    public MockAppender() {
        super();
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        // Eat it
    }

    private String testId;


    public String getTestId() {
        return testId;
    }

    public void setTestId(String testId) {
        if (testId != null) {
            APPENDERS.put(testId, this);
        }
        this.testId = testId;
    }
}
