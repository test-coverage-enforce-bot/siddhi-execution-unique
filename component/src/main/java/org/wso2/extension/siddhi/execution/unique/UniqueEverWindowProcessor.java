/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * this is Unique Ever Window Processor implementation.
 */

// TBD: Annotation description
@Extension(name = "ever", namespace = "unique", description = "TBD", parameters = {
        @Parameter(name = "abc", description = "TBD", type = {
                DataType.STRING }) }, examples = @Example(syntax = "TBD", description = "TBD"))

public class UniqueEverWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ConcurrentHashMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();
    private VariableExpressionExecutor[] variableExpressionExecutors;


    @Override protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
            boolean b, SiddhiAppContext siddhiAppContext) {

        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length];
        for (int i = 0; i < attributeExpressionExecutors.length; i++) {
            variableExpressionExecutors[i] = (VariableExpressionExecutor) attributeExpressionExecutors[i];
        }
    }

    @Override protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner) {
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();

            StreamEvent streamEvent = streamEventChunk.getFirst();
            streamEventChunk.clear();
            while (streamEvent != null) {
                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.EXPIRED);

                StreamEvent oldEvent = map.put(generateKey(clonedEvent), clonedEvent);
                if (oldEvent != null) {
                    oldEvent.setTimestamp(currentTime);
                    streamEventChunk.add(oldEvent);
                }
                StreamEvent next = streamEvent.getNext();
                streamEvent.setNext(null);
                streamEventChunk.add(streamEvent);
                streamEvent = next;
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

    @Override public Map<String, Object> currentState() {
        Map<String, Object> map = new HashMap<>();
        map.put("map", this.map);
        return map;
    }

    @Override public synchronized void restoreState(Map<String, Object> map) {
        this.map = (ConcurrentHashMap<String, StreamEvent>) map.get("map");
    }

    @Override public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, map.values(), streamEventCloner);
        } else {
            return null;
        }
    }


    @Override public CompiledCondition compileCondition(Expression expression,
            MatchingMetaInfoHolder matchingMetaInfoHolder, SiddhiAppContext siddhiAppContext,
            List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, Table> tableMap, String s) {
        return OperatorParser.constructOperator(map.values(), expression, matchingMetaInfoHolder, siddhiAppContext,
                variableExpressionExecutors, tableMap, this.queryName);
    }

    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (VariableExpressionExecutor executor : variableExpressionExecutors) {
            stringBuilder.append(event.getAttribute(executor.getPosition()));
        }
        return stringBuilder.toString();
    }
}
