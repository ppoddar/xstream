package xstream;

import java.io.Serializable;

import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordValue;
import xstream.util.Assert;
import static xstream.TimeSeriesSchema.*;

/**
 * An event is a timestamped tuple. The value at each dimension of the tuple 
 * can be accessed and set by name. The type of value to get or set is a 
 * simple Java type such as int, double or String. 
 * 
 * An event also represents persistent database. The values stored in an
 * Event is specified by the application when written to database,
 * on the other hand, the values may also come from database record
 * when existing events are read.
 * 
 * The types of value are database types.  
 * 
 * 
 * memory are same as  
 * 
 * The property names and types are declared when a time series is defined.
 * 
 * <br>
 * An event is basic element of a {@link TimeSeries}. 
 * An event is not instantiated directly, but implicitly in {@link 
 * WritableTimeSeries#write(long, java.util.Map)} an event is written
 * by supplying its timestamp and values.
 * 
 * @author pinaki poddar
 *
 */
@SuppressWarnings("serial")
public class Event implements Serializable {

    private RecordValue _record;
    private EventMetadata _meta;
    
    /**
     * Creates an event supplying the timestamp and a definition of
     * properties. The event is not populated with any value.
     * 
     * @param ts
     * @param data
     */
    Event(EventMetadata meta, RecordValue record) {
        Assert.assertNotNull(meta, "cannot create event with null definition");
        Assert.assertNotNull(record, "cannot create event with null record");
        _meta = meta;
        TIMESTAMP.assertExistsIn(record);
        _record = record;
    }

    /**
     * gets all values
     * @return all values in an array. The array elements  are in same order
     * as declaration order of properties.
     * The ordering is important for Spark.
     * 
     * The element values in the array are typed. As an event has access
     * to its metadata, a property value can be cast to a specific type
     * sucj as double or integer.
     */
    public Object[] values() {
        Object[] values = new Object[_meta.asRecordDef().getNumFields()];
        for (int i = 0; i < values.length; i++) {
            FieldValue f = _record.get(i);
            if (f.isInteger()) values[i] = f.asInteger().get();
            else if (f.isDouble()) values[i] = f.asDouble().get();
            else if (f.isString()) values[i] = f.asString().get();
            else throw new RuntimeException();
        }
        return values;
    }
    
    
    /**
     * Gets timestamp of the event.
     * @return timestamp of the event. Always positive.
     */
    public long getTimestamp() {
        return TIMESTAMP.getLong(_record);
    }
    
    /**
     * Gets the value of this event as a database record.
     * @return
     */
    RecordValue getRecord() {
        return _record;
    }
    
 //TODO: assert field value type   
    /**
     * Gets the value of given property.
     * two kinds of error : the property is not defined
     *                      the property is defined, 
     *                      but record does not have a value 
     * 
     * @param property name of an event property
     * @return value of the given property
     * @exception IllegalArgumentException if given property is not defined
     */
    public Object get(String property) {
            return _meta.get(property, _record);
        
    }
    
    /**
     * Sets value at a given property.
     * 
     * @param fieldName name of an event property
     * @param value value of the given property
     * @return the same event
     * @exception IllegalArgumentException if given property is not defined
     */
    Event setValue(String propertyName, Object value) {
        _meta.set(propertyName, _record, value);
        
        return this;
    }
    

    
    
    public String toString() {
        return "event-"+ getTimestamp() + ":" + _record.toJsonString(false);
    }
}
