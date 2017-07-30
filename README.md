siddhi-execution-unique
======================================
---
##### New version of Siddhi v4.0.0 is built in Java 8.
##### Latest Released Version v4.0.0-m18.

This is siddhi-execution-unique extension for stream processor to process unique event streams.
This extension has implemented different type of unique windows which hold only unique events based on the given unique key parameter and return current events and expired events.
A window emits current events when a new event arrives and emits expired events when an existing event has expired from a window.

Features Supported
------------------
 - UniqueEverWindow
   * A Window that is updated with the latest events based on a unique key parameter. 
      When a new event that arrives has the same value for the unique key parameter as an existing event,
      the existing event expires, and it is replaced by the later event. 
 - UniqueTimeWindowProcessor
   * A sliding time window that holds the latest unique events that arrived
     during the last window time period. The unique events are determined based on
     the value for a specified unique key parameter. The window is updated with each event arrival and expiry.
     When a new event that arrives within a window time period has the same value
     for the unique key parameter as an existing event in the window,
     the previous event is replaced by the later event.
 - UniqueTimeBatchWindowProcessor
   * Batch (tumbling) time window that is updated with the latest events based
     on a unique key parameter. If a new event that arrives within the window time period has a value for
     the key parameter which matches that of an existing event, the existing event expires and
     it is replaced by the later event. 
 - UniqueLengthWindowProcessor
   * Sliding length window that holds the latest window length unique events according
     to the unique key parameter and gets updated for each event arrival and expiry.
     When a new event arrives with the key that is already there in the window,
     then the previous event is expired and new event is kept within the window.
 - UniqueLengthBatchWindowProcessor
   * Batch (tumbling) window that holds a specified number of latest unique events.
     The unique events are determined based on the value for a specified unique key parameter.
     The window is updated for every window length (i.e., for the last set of events of
     the specified number in a tumbling manner). When a new event that arrives
     within the a window length has the same value for the unique key parameter
     as an existing event is the window, the previous event is replaced by the new event.

 - UniqueFirstWindowProcessor
   * A window that holds only the first unique events that are unique according to the unique
     key parameter. When a new event arrives with a key that is already in the window, 
     that event is not processed by the window.

 - UniqueFirstTimeBatchWindow
   * Batch (tumbling) window that holds the first unique events that
    arrive during the window time period. The unique events to be held are selected based 
    on the value for a specified unique key parameter. If a new event arrives with a value for
    the unique key parameter that is same as that of an existing event in the window,
    the new event is not processed by the window.
 - UniqueFirstLengthBatchWindowProcessor
   * Batch (tumbling) window that holds a specific number of unique events
    (depending on which events arrive first). The unique events are selected based on a specific parameter 
    that is considered the unique key. When a new event arrives with a value for the unique key parameter 
    that matches the same of an existing event in the window, that event is not processed by the window.
 - UniqueExternalTimeBatchWindow
   * Batch (tumbling) time window that is determined based on external time
     (i.e., time stamps specified via an attribute in the events).
     It holds the latest unique events that arrived during the last window time period.
     The unique events are determined based on the value for a specified unique key parameter.
     When a new event arrives within the time window with a value for the unique key parameter
     that is the same as that of an existing event in the window,
     the existing event expires and it is replaced by the later event.
 
  
     
 Prerequisites for using the feature
 ---------------------------------
 - Siddhi Stream should be defined

 
 Deploying the feature
 ---------------------
 Feature can be deploy as a OSGI bundle by putting jar file of component to DAS_HOME/lib directory of DAS 4.0.0 pack. 
 
 
 Example Siddhi Queries
 ----------------------
  define stream CseEventStream (symbol string, price float, volume int)   
  from CseEventStream#window.unique:timeBatch(symbol, 1 sec)  
  select symbol, price, volume  
  insert all events into OutputStream ;
  
  This window holds the latest unique events that arrive from the CseEventStream at a  given time,
  and returns all evens to the OutputStream stream. It is updated every  second based on the latest 
  values for the symbol attribute.
 
 
 How to Contribute
 ----------------
  * Send your bug fixes pull requests to [master branch] 
  (https://github.com/wso2-extensions/siddhi-execution-unique/tree/master) 

 Contact us 
 ---------
  Siddhi developers can be contacted via the mailing lists:
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

 #### We welcome your feedback and contribution.
 
 WSO2 Smart Analytics Team.


