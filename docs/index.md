siddhi-execution-unique
======================================

The **siddhi-execution-unique extension** of WSO2 Stream Processor processes unique event streams.
Different types of unique windows are available to hold unique events based on the given unique key parameter, and return current and expired events.
A window emits current events when new events arrive, and emits expired events when existing events expire.

**Latest Released Version v4.0.2.** 

## Jenkins Build Status 

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-unique/) |

---

## Features Supported
The following types of windows are supported by this extension. For more information about these windows, see [[Site : Siddhi Execution Unique](https://wso2-extensions.github.io/siddhi-execution-unique/)].

* UniqueEverWindow
  * This window is updated with the latest events based on a unique key parameter. 
      When a new event that arrives has the same value for the unique key parameter as an existing event,
      the existing event expires, and it is replaced by the later event. 
* UniqueTimeWindowProcessor
  * This is a sliding time window that holds the latest unique events that arrived
     during the last window time period. The unique events are determined based on
     the value for a specified unique key parameter. The window is updated with each event arrival and expiry.
     When a new event that arrives within a window time period has the same value
     for the unique key parameter as an existing event in the window,
     the previous event is replaced by the later event.
* UniqueTimeBatchWindowProcessor
  * This is a batch (tumbling) time window that is updated with the latest events based
     on a unique key parameter. If a new event that arrives within the window time period has a value for
     the key parameter which matches that of an existing event, the existing event expires and
     it is replaced by the later event. 
* UniqueLengthWindowProcessor
  * This is a sliding length window that holds the latest window length unique events according
     to the unique key parameter and gets updated for each event arrival and expiry.
     When a new event arrives with the key that is already there in the window,
     then the previous event is expired and new event is kept within the window.
* UniqueLengthBatchWindowProcessor
  * This is a batch (tumbling) window that holds a specified number of latest unique events.
     The unique events are determined based on the value for a specified unique key parameter.
     The window is updated for every window length (i.e., for the last set of events of
     the specified number in a tumbling manner). When a new event that arrives
     within the a window length has the same value for the unique key parameter
     as an existing event is the window, the previous event is replaced by the later event.

* UniqueFirstWindowProcessor
  * This window holds only the first unique events that are unique according to the unique
     key parameter. When a new event arrives with a key that is already in the window, 
     that event is not processed by the window.

* UniqueFirstTimeBatchWindow
  * This is a batch (tumbling) window that holds the first unique events that
    arrive during the window time period. The unique events to be held are selected based 
    on the value for a specified unique key parameter. If a new event arrives with a value for
    the unique key parameter that is same as that of an existing event in the window,
    the new event is not processed by the window.
* UniqueFirstLengthBatchWindowProcessor
  * This is a batch (tumbling) window that holds a specific number of unique events
    (depending on which events arrive first). The unique events are selected based on a specific parameter 
    that is considered the unique key. When a new event arrives with a value for the unique key parameter 
    that matches the same of an existing event in the window, that event is not processed by the window.
* UniqueExternalTimeBatchWindow
  * This is a batch (tumbling) time window that is determined based on external time
     (i.e., time stamps specified via an attribute in the events).
     It holds the latest unique events that arrived during the last window time period.
     The unique events are determined based on the value for a specified unique key parameter.
     When a new event arrives within the time window with a value for the unique key parameter
     that is the same as that of an existing event in the window,
     the existing event expires and it is replaced by the later event.
 
  
     
## Prerequisites for using the feature
 
 - In order to use this feature, a Siddhi event stream must be defined.

 
## Deploying the feature
 
 This feature can be deployed as an OSGI bundle by placing the jar file of the component in the DAS_HOME/lib directory.
 
 
## Example Siddhi Queries
 
 * define stream CseEventStream (symbol string, price float, volume int)   
  from CseEventStream#window.unique:timeBatch(symbol, 1 sec)  
  select symbol, price, volume  
  insert all events into OutputStream ;
  
 * This window holds the latest unique events that arrive from the `CseEventStream` at a  given time,
  and returns all the events to the `OutputStream` stream. It is updated every  second based on the latest 
  values for the symbol attribute.
  
 * This extension is used by the Siddhi application deployed in the following sample [[Sample 0013](https://github.com/wso2/product-sp/tree/master/modules/samples/artifacts/0013)] in Wso2 Stream Processor.
 
## Related works
 * WSO2 Stream Processor [[Product SP](https://docs.wso2.com/display/SP400/)]. 
 * Siddhi Complex Event Processing Engine [[Siddhi CEP](Siddhi Complex Event Processing Engine)]
 
## How to Contribute
 
  * Please report issues at [Siddhi Github Issue Tacker](https://github.com/wso2-extensions/siddhi-execution-unique/issues)
  * Send your bug fixes pull requests to [master branch](https://github.com/wso2-extensions/siddhi-execution-unique/tree/master) 

## Contact us 
 **Siddhi developers can be contacted via the mailing lists:**
 
  * Carbon Developers List   : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

## We welcome your feedback and contribution

 *WSO2 Stream Processor Team.*



## API Docs:

1. <a href="./api/4.0.2-SNAPSHOT.md">4.0.2-SNAPSHOT</a>
