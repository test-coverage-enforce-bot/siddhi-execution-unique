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

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * class representing unique ever window processor test case.
 */
public class UniqueEverWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueEverWindowTestCase.class);
    private long value;
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger eventCount;

    @BeforeMethod public void init() {
        value = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);

    }

    @Test public void uniqueEverWindowTest1() throws InterruptedException {
        log.info("uniqueEverWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:ever(ip) "
                + "select count(ip) as ipCount, ip " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }
                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(0);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.4" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.5" });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(value, 3, "Event max value");

        siddhiAppRuntime.shutdown();
    }

    @Test public void uniqueEverWindowTest2() throws InterruptedException {
        log.info("uniqueEverWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (id String , timeStamp long, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:ever(ip) "
                + "select id, count(ip) as ipCount, ip " + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                }

                if (inEvents != null) {
                    value = (Long) inEvents[inEvents.length - 1].getData(1);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { "A1", System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { "A2", System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { "A3", System.currentTimeMillis(), "192.10.1.4" });
        inputHandler.send(new Object[] { "A4", System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { "A5", System.currentTimeMillis(), "192.10.1.5" });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(value, 3, "Event max value");

        siddhiAppRuntime.shutdown();
    }

}
