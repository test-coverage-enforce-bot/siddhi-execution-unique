/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.unique;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * UniqueLengthBatch Window
 * Sample Query:
 * from inputStream#window.unique:lengthBatch(attribute1,3,true)
 * select attribute1, attribute2
 * insert into outputStream;
 *
 * Description:
 * The behavior of this window is similar to that of length-batch-window in Siddhi,
 * except that this window keeps only "unique" events in the window.
 * The window will drop any duplicate events.
 * We call two events are "not unique" if a certain attribute
 * (e.g. attribute1 in the sample query given) bares equal values in both the events.
 *
 * In the given example query, 3 is the length of the window.
 * attribute1 is the attribute which is checked for uniqueness.
 * Third boolean parameter (which is optional - defaults to false, if not provided),
 * being set to 'true', indicates that this is a "first-unique" window.
 * When a duplicate event arrives, "first-unique" window keeps the first event that came in
 * and drops the event which came later.
 * If the third parameter is set to 'false', then the window is a "last-unique" window;
 * meaning, when a duplicate event arrives,
 * the last-unique window keeps the event which came later and drops the event which came before.
 *
 * @since 1.0.0
 */

/**
 * class representing unique length batch window processor implementation.
 */

// TBD: Annotation description
@Extension(name = "lengthBatch", namespace = "unique", description = "TBD", parameters = {
        @Parameter(name = "abc.efg.hij", description = "TBD", type = {
                DataType.STRING }) }, examples = @Example(syntax = "TBD", description = "TBD"))

public class UniqueLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {

    private int windowLength;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired = null;
    private SiddhiAppContext siddhiAppContext;
    private StreamEvent resetEvent = null;
    private VariableExpressionExecutor uniqueKey;
    private Map<Object, StreamEvent> uniqueEventMap = new HashMap<>();


    @Override protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
            boolean b, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.eventsToBeExpired = new ComplexEventChunk<>(false);
        if (attributeExpressionExecutors.length == 2) {
            if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
                this.uniqueKey = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            } else {
                throw new SiddhiAppValidationException("Unique Length Batch window should have variable "
                        + "for Unique Key parameter but found an attribute " + attributeExpressionExecutors[0]
                        .getClass().getCanonicalName());
            }
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    this.windowLength = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue());
                } else {
                    throw new SiddhiAppValidationException(
                            "Unique Length Batch window's Length parameter should be INT, but found "
                                    + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException("Unique Length Batch window should have constant "
                        + "for Length parameter but found a dynamic attribute " + attributeExpressionExecutors[1]
                        .getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Unique Length batch window should only have two parameters, " + "but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }

    }


    @Override protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                addUniqueEvent(uniqueEventMap, uniqueKey, clonedStreamEvent);
                if (uniqueEventMap.size() == windowLength) {
                    for (StreamEvent event : uniqueEventMap.values()) {
                        event.setTimestamp(currentTime);
                        currentEventChunk.add(event);
                    }
                    uniqueEventMap.clear();
                    if (eventsToBeExpired.getFirst() != null) {
                        while (eventsToBeExpired.hasNext()) {
                            StreamEvent expiredEvent = eventsToBeExpired.next();
                            expiredEvent.setTimestamp(currentTime);
                        }
                        outputStreamEventChunk.add(eventsToBeExpired.getFirst());
                    }
                    eventsToBeExpired.clear();
                    if (currentEventChunk.getFirst() != null) {
                        // add reset event in front of current events
                        outputStreamEventChunk.add(resetEvent);
                        currentEventChunk.reset();
                        while (currentEventChunk.hasNext()) {
                            StreamEvent toExpireEvent = currentEventChunk.next();
                            StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(toExpireEvent);
                            eventClonedForMap.setType(StreamEvent.Type.EXPIRED);
                            eventsToBeExpired.add(eventClonedForMap);
                        }
                        resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                        resetEvent.setType(ComplexEvent.Type.RESET);
                        outputStreamEventChunk.add(currentEventChunk.getFirst());
                    }
                    currentEventChunk.clear();
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    protected void addUniqueEvent(Map<Object, StreamEvent> uniqueEventMap, VariableExpressionExecutor uniqueKey,
            StreamEvent clonedStreamEvent) {
        uniqueEventMap.put(clonedStreamEvent.getAttribute(uniqueKey.getPosition()), clonedStreamEvent);
    }

    @Override public void start() {
        //Do nothing
    }

    @Override public void stop() {
        //Do nothing
    }

    @Override public Map<String, Object> currentState() {
        if (eventsToBeExpired != null) {
            Map<String, Object> map = new HashMap<>();
            map.put("currentEventChunk", currentEventChunk.getFirst());
            map.put("eventsToBeExpired", eventsToBeExpired.getFirst());
            map.put("count", count);
            map.put("resetEvent", resetEvent);
            return map;
        } else {
            Map<String, Object> map = new HashMap<>();
            map.put("currentEventChunk", currentEventChunk.getFirst());
            map.put("count", count);
            map.put("resetEvent", resetEvent);
            return map;
        }
    }

    @Override public void restoreState(Map<String, Object> map) {
        if (map.size() > 3) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) map.get("currentEventChunk"));
            eventsToBeExpired.clear();
            eventsToBeExpired.add((StreamEvent) map.get("eventsToBeExpired"));
            count = (Integer) map.get("count");
            resetEvent = (StreamEvent) map.get("resetEvent");
        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) map.get("currentEventChunk"));
            count = (Integer) map.get("count");
            resetEvent = (StreamEvent) map.get("resetEvent");
        }
    }

    @Override public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, uniqueEventMap.values(), streamEventCloner);
        } else {
            return null;
        }

    }

    @Override public CompiledCondition compileCondition(Expression expression,
            MatchingMetaInfoHolder matchingMetaInfoHolder, SiddhiAppContext siddhiAppContext,
            List<VariableExpressionExecutor> list, Map<String, Table> map, String s) {
        return OperatorParser
                .constructOperator(uniqueEventMap.values(), expression, matchingMetaInfoHolder, siddhiAppContext, list,
                        map, this.queryName);
    }
}
