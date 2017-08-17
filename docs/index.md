siddhi-execution-unique
======================================

The **siddhi-execution-unique extension** is an extension to [Siddhi](https://wso2.github.io/siddhi) to process event streams based on unique events.
Different types of unique windows are available to hold unique events based on the given unique key parameter.

* Source code : [https://github.com/wso2-extensions/siddhi-execution-unique](https://github.com/wso2-extensions/siddhi-execution-unique)
* Releases : [https://github.com/wso2-extensions/siddhi-execution-unique/releases](https://github.com/wso2-extensions/siddhi-execution-unique/releases)
* Issue tracker :  [https://github.com/wso2-extensions/siddhi-execution-unique/issues](https://github.com/wso2-extensions/siddhi-execution-unique/issues)

## How to use 

**Using extension in [WSO2 Stream Processor](https://github.com/wso2/product-sp/releases)**

* You can use the extension with the latest [WSO2 Stream Processor](https://github.com/wso2/product-sp/releases) with editor, debugger and simulation support. 

* Deployed to WSO2 Stream Processor by placing the component [jar](https://github.com/wso2-extensions/siddhi-execution-unique/releases) in STREAM_PROCESSOR_HOME/lib directory.

**Using extension as a [java library](https://wso2.github.io/siddhi/documentation/running-as-a-java-library/)**

* Extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <depandancy>
        <groupId>org.wso2.extension.siddhi.execution.unique</groupId>
        <artifactId>siddhi-execution-unique-parent</artifactId>
        <version>x.x.x</version>
     </depandancy>
```





## Jenkins Build Status 

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/) |

---
  

## Features

* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#timebatch-window">timeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) time window that is updated with the latest events based on a unique key parameter. If a new event that arrives within the window time period has a value for the key parameter which matches that of an existing event, the existing event expires and it is replaced by the later event. </p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#ever-window">ever</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a  window that is updated with the latest events based on a unique key parameter. When a new event that arrives, has the same value for the unique key parameter as an existing event, the existing event expires, and it is replaced by the later event.</p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#length-window">length</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a sliding length window that holds the latest window length unique events according to the unique key parameter and gets updated for each event arrival and expiry. When a new event arrives with the key that is already there in the window, then the previous event is expired and new event is kept within the window.</p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#firstlengthbatch-window">firstLengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) window that holds a specific number of unique events (depending on which events arrive first). The unique events are selected based on a specific parameter that is considered as the unique key. When a new event arrives with a value for the unique key parameter that matches the same of an existing event in the window, that event is not processed by the window.</p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#externaltimebatch-window">externalTimeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) time window that is determined based on external time (i.e., time stamps specified via an attribute in the events). It holds the latest unique events that arrived during the last window time period. The unique events are determined based on the value for a specified unique key parameter. When a new event arrives within the time window with a value for the unique key parameter that is the same as that of an existing event in the window, the existing event expires and it is replaced by the later event.</p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#firsttimebatch-window">firstTimeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A batch (tumbling) time window that holds first unique events according to the unique key parameter that have arrived during window time period and gets updated for each window time period. When a new event arrives with a key which is already in the window, that event is not processed by the window.</p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#first-window">first</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a window that holds only the first unique events that are unique according to the unique key parameter. When a new event arrives with a key that is already in the window, that event is not processed by the window.</p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#time-window">time</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a sliding time window that holds the latest unique events that arrived during the last window time period. The unique events are determined based on the value for a specified unique key parameter. The window is updated with each event arrival and expiry. When a new event that arrives within a window time period has the same value for the unique key parameter as an existing event in the window, the previous event is replaced by the later event.</p></div>
* <a target="_blank" href="./api/4.0.2-SNAPSHOT/#lengthbatch-window">lengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This is a batch (tumbling) window that holds a specified number of latest unique events. The unique events are determined based on the value for a specified unique key parameter. The window is updated for every window length (i.e., for the last set of events of the specified number in a tumbling manner). When a new event that arrives within the a window length has the same value for the unique key parameter as an existing event is the window, the previous event is replaced by the new event.</p></div>

## How to Contribute
 
  * Please report issues at [Github Issue Tacker](https://github.com/wso2-extensions/siddhi-execution-unique/issues).
  * Send your bug fixes pull requests to [master branch](https://github.com/wso2-extensions/siddhi-execution-unique/tree/master). 
 
## Contact us 
 * Post your questions on http://stackoverflow.com/ tagging ["siddhi"](http://stackoverflow.com/search?q=siddhi)
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
