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
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
* Sample Query:
* from inputStream#window.unique:length(attribute1,3)
* select attribute1, attribute2
* insert into outputStream;
*
* Description:
* In the example query given, 3 is the length of the window and attribute1 is the unique attribute.
* According to the given attribute it will give unique events within given length.
* */

/**
 * class representing unique length window processor implementation.
 */

//TBD:annotation description
@Extension(name = "length", namespace = "unique", description = "TODO", parameters = {
        @Parameter(name = "parameter", description = "TODO", type = {
                DataType.STRING }) }, examples = @Example(syntax = "TODO", description = "TODO"))

public class UniqueLengthWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ConcurrentHashMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();
    private VariableExpressionExecutor[] variableExpressionExecutors;
    private int length;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;

    @Override protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
            boolean b, SiddhiAppContext siddhiAppContext) {
        expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
        if (attributeExpressionExecutors.length == 2) {
            variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            length = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
        } else {
            throw new SiddhiAppValidationException("Unique Length window should only have two parameters "
                    + "(<string|int|bool|long|double|float> attribute, <int> windowLength), but found "
                    + attributeExpressionExecutors.length + " input attributes");
        }

    }

    @Override protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner) {
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                streamEvent.setNext(null);
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);
                StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(clonedEvent);
                StreamEvent oldEvent = map.put(generateKey(eventClonedForMap), eventClonedForMap);
                if (oldEvent == null) {
                    count++;
                }
                if ((count <= length) && (oldEvent == null)) {
                    this.expiredEventChunk.add(clonedEvent);
                } else {
                    if (oldEvent != null) {
                        while (expiredEventChunk.hasNext()) {
                            StreamEvent firstEventExpired = expiredEventChunk.next();
                            if (firstEventExpired.equals(oldEvent)) {
                                this.expiredEventChunk.remove();
                            }
                        }
                        this.expiredEventChunk.add(clonedEvent);
                        streamEventChunk.insertBeforeCurrent(oldEvent);
                        oldEvent.setTimestamp(currentTime);
                    } else {
                        StreamEvent firstEvent = this.expiredEventChunk.poll();
                        if (firstEvent != null) {
                            firstEvent.setTimestamp(currentTime);
                            streamEventChunk.insertBeforeCurrent(firstEvent);
                            this.expiredEventChunk.add(clonedEvent);
                        } else {
                            streamEventChunk.insertBeforeCurrent(clonedEvent);
                        }
                    }
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override public void start() {
        //Do nothing
    }

    @Override public void stop() {
        //Do nothing
    }

    @Override public synchronized Map<String, Object> currentState() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("expiredEventChunk", expiredEventChunk.getFirst());
        map.put("count", count);
        map.put("map", this.map);
        return map;
    }

    @Override public synchronized void restoreState(Map<String, Object> map) {
        expiredEventChunk.clear();
        expiredEventChunk.add((StreamEvent) map.get("expiredEventChunk"));
        count = (Integer) map.get("count");
        this.map = (ConcurrentHashMap) map.get("map");
    }

    @Override public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, expiredEventChunk, streamEventCloner);
        } else {
            return null;
        }
    }

    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (VariableExpressionExecutor executor : variableExpressionExecutors) {
            stringBuilder.append(event.getAttribute(executor.getPosition()));
        }
        return stringBuilder.toString();
    }

    @Override public CompiledCondition compileCondition(Expression expression,
            MatchingMetaInfoHolder matchingMetaInfoHolder, SiddhiAppContext siddhiAppContext,
            List<VariableExpressionExecutor> list, Map<String, Table> map, String s) {
        return OperatorParser
                .constructOperator(expiredEventChunk, expression, matchingMetaInfoHolder, siddhiAppContext, list, map,
                        this.queryName);
    }
}
