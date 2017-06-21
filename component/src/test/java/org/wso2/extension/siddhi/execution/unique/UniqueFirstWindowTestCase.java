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
 * class representing unique first window test case.
 */

public class UniqueFirstWindowTestCase {
    private static final Logger log = Logger.getLogger(UniqueFirstWindowTestCase.class);
    private int count;
    private boolean eventArrived;
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger eventCount;

    @BeforeMethod public void init() {
        count = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);
    }

    @Test public void uniqueFirstWindowTest1() throws InterruptedException {
        log.info("UniqueFirstWindow test1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:first(ip) " + "select ip "
                + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    count = count + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.fail("Remove events emitted");
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

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(count, 3, "Number of output event value");
        siddhiAppRuntime.shutdown();

    }

    @Test public void firstUniqueWindowTest2() throws InterruptedException {
        log.info("UniqueFirstWindow test2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" + "define stream LoginEvents (timeStamp long, ip string);";
        String query = "" + "@info(name = 'query1') " + "from LoginEvents#window.unique:first(ip) " + "select ip "
                + "insert into uniqueIps ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventCount.incrementAndGet();
                if (inEvents != null) {
                    count = count + inEvents.length;
                }
                if (removeEvents != null) {
                    Assert.fail("Remove events emitted");
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("LoginEvents");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.12" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });
        inputHandler.send(new Object[] { System.currentTimeMillis(), "192.10.1.3" });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(count, 2, "Number of output event value");
        siddhiAppRuntime.shutdown();

    }

}
