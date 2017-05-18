# TimeSeries in Oracle NoSQL database


A **timeSeries** is a sequence of events ordered by time. 
An **event** is any data with a timestamp.


TimeSeries occurs in many data
management scenarios in financial business, sensor data acquisition, 
system monitoring etc. 

## Storing Timeseries

While storing events is not new, the need to collect and analyze massive 
amounts of sequenced, often unstructured data from thousands or more devices 
is a new and growing requirement for IoT.

We consider timeseries with following characteristics: 
+ contains large number of events (could be hundreds of millions)
+ time is as essential dimension
+ events carry arbitrary properties and flexible in structure
+ the information content of events are more meaningful as aggregate 
than of an individual event. 
  
  
### Storage Layout

We use NoSQL database to store a timeseries. A timeseries corresponds to a
database table where each row of the table represents a  *time slot*.

A *slot* is a sequence of fixed number of events ordered by their timestamp.
Each event has a set of properties which are defined at creation of the timeseries.

The layout of a table that represents a TimeSeries
 
| Column            | Type         |       Description                  
| :-----------      | :----------  | : ---------------------------------  
|**SLOT_INDEX**     | INTEGER      | index of the slot  (primary key)   
| TIMESTAMPT_START  | LONG         | timestamp of first active event    
| TIMESTAMP_END     | LONG         | timestamp of last active event     
| EVENT_INDEX_OFFSET| INTEGER      | index of first active event        
| EVENT_INDEX_END   | INTEGER      | index of first active event        
| EVENT_LIMIT       | INTEGER      | total number of events in a slot   
| EVENTS            | ARRAY(RECORD)| data as array of event             |    

However, such tables are managed by `xstream` and completely  transparent to
 user application. 
 
#### Time Slot

A time slot is a sequence of events ordered by time. The event data of a
slot plus metadata about the slot forms a row  a `timeseries`.   

The events in a slot are maintained as a array of records. Each element
record of the array correspond to an event with timestamp. The fields
of an event is determined by user application. An event can have any
property, however, all events in a slot must have same structure.

**Timerange** of a slot is the time interval `(TIMESTAMPT_START, TIMESTAMPT_END)`


+ `SLOT_INDEX` is used as primary key, but it is noteworthy that
 the events of the entire series are *not* ordered by slot index. In other
 words two slots indices `i` and `j` where `j > i` does not necessarily imply 
 the time range
 of `j` is after that of `i`. 
 
+ The time ranges of slots are not overlapping.
 
+ A slot contains fixed number events, though individual events can vary
in storage bytes. So slots can vary in their storage bytes.

### Data Locality

The data locality is achieved through slot-based layout of a 'timeseries`.  
Because a) the timestamp of 
events in a slot are monotonic and b) the time range of slots are
non-overlapping.

 


## Query

Apache Spark runs SQL queries on timeseries. 
The query syntax is same as the syntax supported by Apache Spark, except
joining between two timeseries is not supported. 

A timeseries persistent in Oracle NoSQL database is presented to Apache
Spark framework as a  `TimeSeriesRDD`  (RDD stands for Resilient Distributed Dataset
-- the primary data abstraction used by Apache Spark for cluster computing). 
Each partition of `TimeSeriesRDD` corresponds to one or more slots
of persistent timeseries.  

Apache Spark can partition and distribute across distributes computation tasks 
across a clusters of machines and finally combine the results. 
Once timeseries data is made available 
as an RDD with partitions, Apache Spark evaluates any SQL query 
(including aggregate query) using same powerful cluster computing mechanism.

The following code snippet demonstrates how a persistent TimeSeries can be
queried using Spark:

```java 

	import xstream.spark.TimeseriesRDD;
	import org.apache.spark.sql.SparkSession;
	import org.apache.spark.sql.Row;
	// open a Spark Session in standard manner
    	SparkSession spark = ...                                
    	// use an URI for NoSQL persistent TimeSeries
	String url = "nosql://localhost:5000/kvstore/traffic";  
	// create an RDD from a persistent TimeSeries
	TimeSeriesRDD = new TimeseriesRDD(spark, url);   
	// declare a SQL aggregate query       
    	String sqlText = "select avg(speed) from "              
                + rdd.getSeriesName()  + "  where avg(speed) > 70";
      // execute query for results as Spark Row          
    	Dataset<Row> rs = spark.executeQuery(sqlText);          

```


