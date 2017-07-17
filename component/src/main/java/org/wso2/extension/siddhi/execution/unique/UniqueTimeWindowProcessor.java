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
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/*
* Sample Query:
* from inputStream#window.unique:time(attribute1,3 sec)
* select attribute1, attribute2
* insert into outputStream;
*
* Description:
* In the example query given, 3 is the duration of the window and attribute1 is the unique attribute.
* According to the given attribute it will give unique events within given time.
* */

/**
 * class representing unique time window processor implementation.
 */

@Extension(
        name = "time",
        namespace = "unique",
        description = "A sliding time window that holds latest unique events"
                + " according to the given unique key parameter "
                + " that have arrived during the last window time period"
                + " and gets updated for each event arrival and expiry." ,

        parameters = {
                @Parameter(name = "unique.key",
                        description = "The attribute that should be checked for uniqueness.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME,
                                DataType.BOOL, DataType.DOUBLE}),
                @Parameter(name = "window.time",
                        description = "The sliding time period for which the window should hold events.",
                        type = {DataType.INT, DataType.LONG, DataType.TIME})
        },
        examples = {
                @Example(
                        syntax = "define stream cseEventStream (symbol string, price float, volume int)\n" +
                                 "from cseEventStream#window.unique:time(symbol, 1 sec)\n" +
                                 "select symbol, price, volume\n" +
                                 "insert expired events into outputStream ;",

                        description = "This will process latest unique events based on symbol that arrived "
                                + " within the last second from the cseEventStream and "
                                + " return the all expired events to the outputStream."
                )
        }
)

public class UniqueTimeWindowProcessor extends WindowProcessor implements SchedulingProcessor, FindableProcessor {

    private ConcurrentMap<String, StreamEvent> map = new ConcurrentHashMap<String, StreamEvent>();
    private long timeInMilliSeconds;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;
    private Scheduler scheduler;
    private SiddhiAppContext siddhiAppContext;
    private volatile long lastTimestamp = Long.MIN_VALUE;
    private VariableExpressionExecutor[] variableExpressionExecutors;


    @Override public synchronized Scheduler getScheduler() {
        return scheduler;
    }

    @Override public synchronized void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
            boolean outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
        if (attributeExpressionExecutors.length == 2) {
            variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();

                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException(
                            "UniqueTime window's parameter time should be either" + " int or long, but found "
                                    + attributeExpressionExecutors[0].getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException(
                        "UniqueTime window should have constant for time parameter but " + "found a dynamic attribute "
                                + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException("UniqueTime window should only have two parameters "
                    + "(<string|int|bool|long|double|float> unique attribute, <int|long|time> windowTime), but found "
                    + attributeExpressionExecutors.length + " input attributes");
        }
    }

    @Override protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
                StreamEvent oldEvent = null;
                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    clonedEvent.setType(StreamEvent.Type.EXPIRED);
                    StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(streamEvent);
                    eventClonedForMap.setType(StreamEvent.Type.EXPIRED);
                    oldEvent = map.put(generateKey(eventClonedForMap), eventClonedForMap);
                    this.expiredEventChunk.add(clonedEvent);
                    if (lastTimestamp < clonedEvent.getTimestamp()) {
                        if (scheduler != null) {
                            scheduler.notifyAt(clonedEvent.getTimestamp() + timeInMilliSeconds);
                            lastTimestamp = clonedEvent.getTimestamp();
                        }
                    }
                }
                expiredEventChunk.reset();
                while (expiredEventChunk.hasNext()) {
                    StreamEvent expiredEvent = expiredEventChunk.next();
                    long timeDiff = expiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0 || oldEvent != null) {
                        if (oldEvent != null) {
                            if (expiredEvent.equals(oldEvent)) {
                                this.expiredEventChunk.remove();
                                streamEventChunk.insertBeforeCurrent(oldEvent);
                                oldEvent.setTimestamp(currentTime);
                                oldEvent = null;
                            }
                        } else {
                            expiredEventChunk.remove();
                            expiredEvent.setTimestamp(currentTime);
                            streamEventChunk.insertBeforeCurrent(expiredEvent);
                            expiredEvent.setTimestamp(currentTime);
                            expiredEventChunk.reset();
                        }
                    } else {
                        break;
                    }
                }
                expiredEventChunk.reset();
                if (streamEvent.getType() != StreamEvent.Type.CURRENT) {
                    streamEventChunk.remove();
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (compiledCondition instanceof Operator) {
            return ((Operator) compiledCondition).find(matchingEvent, expiredEventChunk, streamEventCloner);
        } else {
            return null;
        }
    }

    @Override public CompiledCondition compileCondition(Expression expression,
            MatchingMetaInfoHolder matchingMetaInfoHolder, SiddhiAppContext siddhiAppContext,
            List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, Table> tableMap,
            String queryName) {
        return OperatorParser.constructOperator(expiredEventChunk, expression, matchingMetaInfoHolder, siddhiAppContext,
                variableExpressionExecutors, tableMap, this.queryName);
    }

    @Override public void start() {
        //Do nothing
    }

    @Override public void stop() {
        //Do nothing
    }

    @Override public Map<String, Object> currentState() {
        Map<String, Object> map = new HashMap<>();
        map.put("expiredEventchunck", expiredEventChunk.getFirst());
        map.put("map", this.map);
        return map;
    }

    @Override public void restoreState(Map<String, Object> map) {
        expiredEventChunk.clear();
        expiredEventChunk.add((StreamEvent) map.get("expiredEventchunck"));
        this.map = (ConcurrentMap) map.get("map");
    }

    /**
     * Used to generate key in map to get the old event for current event. It will map key which we give as unique
     * attribute with the event
     *
     * @param event the stream event that need to be processed
     *
     */
    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (VariableExpressionExecutor executor : variableExpressionExecutors) {
            stringBuilder.append(event.getAttribute(executor.getPosition()));
        }
        return stringBuilder.toString();
    }

}
