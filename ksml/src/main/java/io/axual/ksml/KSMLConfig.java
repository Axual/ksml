package io.axual.ksml;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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


import io.axual.ksml.execution.ErrorHandler;
import io.axual.ksml.notation.NotationLibrary;
import lombok.Builder;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;

/**
 * Configuration for generating and running KSML definitions.
 */
@InterfaceStability.Evolving
public record KSMLConfig(String sourceType, String workingDirectory, String configDirectory, Object source,
                         NotationLibrary notationLibrary, ErrorHandler consumeErrorHandler,
                         ErrorHandler produceErrorHandler, ErrorHandler processErrorHandler) {

    @Builder
    public KSMLConfig(String sourceType, String workingDirectory, String configDirectory, Object source, NotationLibrary notationLibrary, ErrorHandler consumeErrorHandler, ErrorHandler produceErrorHandler, ErrorHandler processErrorHandler) {
        this.sourceType = sourceType != null ? sourceType : "file";
        this.workingDirectory = workingDirectory;
        this.configDirectory = configDirectory;
        this.source = source;
        this.notationLibrary = notationLibrary != null ? notationLibrary : new NotationLibrary(new HashMap<>());
        this.consumeErrorHandler = consumeErrorHandler != null ? consumeErrorHandler : new ErrorHandler(true, false, "ConsumeError", ErrorHandler.HandlerType.STOP_ON_FAIL);
        this.produceErrorHandler = produceErrorHandler != null ? produceErrorHandler : new ErrorHandler(true, false, "ProduceError", ErrorHandler.HandlerType.STOP_ON_FAIL);
        this.processErrorHandler = processErrorHandler != null ? processErrorHandler : new ErrorHandler(true, false, "ProcessError", ErrorHandler.HandlerType.STOP_ON_FAIL);
    }
}
