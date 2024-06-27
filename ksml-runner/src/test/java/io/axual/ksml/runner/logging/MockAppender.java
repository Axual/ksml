package io.axual.ksml.runner.logging;

/*-
 * ========================LICENSE_START=================================
 * KSML Runner
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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
