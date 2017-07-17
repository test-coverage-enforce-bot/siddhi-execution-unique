/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.unique;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;

import java.util.Map;

/**
 *  class representing unique first length batch window processor implementation.
 */

@Extension(
        name = "firstLengthBatch",
        namespace = "unique",
        description = "A batch (tumbling) length window that holds window length unique events which arrive first"
                + " according to the unique key parameter and gets updated"
                + " on every window length unique events have arrived."
                + " When a new event arrives with a key which is already in the window,"
                + " that event is not processed by the window." ,

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "window.length",
                        description = "The number of events the window should tumble.",
                        type = {DataType.INT}),
        },
        examples = {
                @Example(
                        syntax = "define window cseEventWindow (symbol string, price float, volume int) " +
                                "from cseEventStream#window.unique:firstLengthBatch(symbol, 10)\n" +
                                "select symbol, price, volume\n" +
                                "insert all events into outputStream ;",
                        description = "This will hold first 10 unique events as a batch from the cseEventWindow"
                                + " based on symbol and return all events to outputStream."
                )
        }
)

public class UniqueFirstLengthBatchWindowProcessor extends UniqueLengthBatchWindowProcessor {
    @Override protected void addUniqueEvent(Map<Object, StreamEvent> uniqueEventMap,
            VariableExpressionExecutor uniqueKey, StreamEvent clonedStreamEvent) {
        if (!uniqueEventMap.containsKey(clonedStreamEvent.getAttribute(uniqueKey.getPosition()))) {
            uniqueEventMap.put(clonedStreamEvent.getAttribute(uniqueKey.getPosition()), clonedStreamEvent);
        }
    }
}
